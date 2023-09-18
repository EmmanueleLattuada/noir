use std::fmt::Display;
use std::fs::File;
use std::io::BufRead;
use std::io::Seek;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::block::Replication;
use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::ExecutionMetadata;
use crate::Stream;
use crate::scheduler::OperatorId;

use super::SnapshotGenerator;

/// Source that reads a text file line-by-line.
///
/// The file is divided in chunks and is read concurrently by multiple replicas.
#[derive(Debug)]
pub struct FileSource {
    path: PathBuf,
    // reader is initialized in `setup`, before it is None
    reader: Option<BufReader<File>>,
    current: usize,
    end: usize,
    terminated: bool,
    operator_coord: OperatorCoord,
    snapshot_generator: SnapshotGenerator,
    persistency_service: Option<PersistencyService<FileSourceState>>,
}

impl Display for FileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileSource<{}>", std::any::type_name::<String>())
    }
}

impl FileSource {
    /// Create a new source that reads the lines from a text file.
    ///
    /// The file is partitioned into as many chunks as replicas, each replica has to have the
    /// **same** file in the same path. It is guaranteed that each line of the file is emitted by
    /// exactly one replica.
    ///
    /// **Note**: the file must be readable and its size must be available. This means that only
    /// regular files can be read.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::FileSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let source = FileSource::new("/datasets/huge.txt");
    /// let s = env.stream(source);
    /// ```
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            path: path.into(),
            reader: Default::default(),
            current: 0,
            end: 0,
            terminated: false,
            // This is the first operator in the chain so operator_id = 0
            // Other fields will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
            snapshot_generator: SnapshotGenerator::new(),
            persistency_service: None,
        }
    }
}

impl Source<String> for FileSource {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct FileSourceState {
    current: u64,
}

impl Operator<String> for FileSource {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        let global_id = metadata.global_id;
        let instances = metadata.replicas.len();

        self.operator_coord.from_coord(metadata.coord);
        let mut last_position = None;
        if let Some(pb) = &metadata.persistency_builder {
            let p_service =pb.generate_persistency_service::<FileSourceState>();
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if let Some(snap_id) = snapshot_id {
                // Get the persisted state
                let opt_state: Option<FileSourceState> = p_service.get_state(self.operator_coord, snap_id.clone());
                if let Some(state) = opt_state {
                    self.terminated = snap_id.clone().terminate();
                    last_position = Some(state.current);
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
                self.snapshot_generator.restart_from(snap_id);
            }
            if let Some(snap_freq) = p_service.snapshot_frequency_by_item {
                self.snapshot_generator.set_item_interval(snap_freq);
            }
            if let Some(snap_freq) = p_service.snapshot_frequency_by_time {
                self.snapshot_generator.set_time_interval(snap_freq);
            }
            self.persistency_service = Some(p_service);
        }

        let file = File::open(&self.path).unwrap_or_else(|err| {
            panic!(
                "FileSource: error while opening file {:?}: {:?}",
                self.path, err
            )
        });
        let file_size = file.metadata().unwrap().len() as usize;

        let range_size = file_size / instances;
        let mut start = range_size * global_id as usize;
        self.current = start;
        self.end = if global_id as usize == instances - 1 {
            file_size
        } else {
            start + range_size
        };

        // Set start to last position (from persisted state), if any
        if last_position.is_some() {
            start = last_position.clone().unwrap() as usize;
            self.current = start;
        }

        let mut reader = BufReader::new(file);
        // Seek reader to the first byte to be read
        reader
            .seek(SeekFrom::Current(start as i64))
            .expect("seek file");
        if global_id != 0 && last_position.is_none() {
            // discard first line
            let mut v = Vec::new();

            self.current += reader
                .read_until(b'\n', &mut v)
                .expect("Cannot read line from file");
        }
        self.reader = Some(reader);

    }

    fn next(&mut self) -> StreamElement<String> {
        if self.terminated {
            if self.persistency_service.is_some() {
                // Save terminated state
                let state = FileSourceState{
                    current: self.current as u64,
                }; 
                self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);            
            }
            log::trace!("terminate {}", self.operator_coord.get_coord());
            return StreamElement::Terminate;
        }
        if self.persistency_service.is_some(){
            // Check snapshot generator
            let snapshot = self.snapshot_generator.get_snapshot_marker();
            if snapshot.is_some() {
                let snapshot_id = snapshot.unwrap();
                // Save state and forward snapshot marker
                let state = FileSourceState{
                    current: self.current as u64,
                }; 
                self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id.clone(), state);
                return StreamElement::Snapshot(snapshot_id);
            }
        }
        let element = if self.current <= self.end {
            let mut line = String::new();
            match self
                .reader
                .as_mut()
                .expect("BufReader was not initialized")
                .read_line(&mut line)
            {
                Ok(len) if len > 0 => {
                    self.current += len;
                    StreamElement::Item(line)
                }
                Ok(_) => {
                    self.terminated = true;
                    StreamElement::FlushAndRestart
                }
                Err(e) => panic!("Error while reading file: {e:?}",),
            }
        } else {
            self.terminated = true;
            StreamElement::FlushAndRestart
        };

        element
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<String, _>("FileSource");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl Clone for FileSource {
    fn clone(&self) -> Self {
        assert!(
            self.reader.is_none(),
            "FileSource must be cloned before calling setup"
        );
        FileSource {
            path: self.path.clone(),
            reader: None,
            current: 0,
            end: 0,
            terminated: false,
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
            snapshot_generator: self.snapshot_generator.clone(),
            persistency_service: self.persistency_service.clone(),
        }
    }
}

impl crate::StreamEnvironment {
    /// Convenience method, creates a `FileSource` and makes a stream using `StreamEnvironment::stream`
    pub fn stream_file<P: Into<PathBuf>>(&mut self, path: P) -> Stream<String, FileSource> {
        let source = FileSource::new(path);
        self.stream(source)
    }
}
