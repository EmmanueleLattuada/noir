use std::fmt::Display;
use std::fs::File;
use std::io::BufRead;
use std::io::Seek;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::Coord;
use crate::network::OperatorCoord;
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::persistency::PersistencyService;
use crate::persistency::PersistencyServices;
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
    coord: Option<Coord>,
    operator_coord: OperatorCoord,
    snapshot_generator: SnapshotGenerator,
    persistency_service: PersistencyService,
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
            coord: None,
            // This is the first operator in the chain so operator_id = 0
            // Other fields will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
            snapshot_generator: SnapshotGenerator::new(),
            persistency_service: PersistencyService::default(),
        }
    }
}

impl Source<String> for FileSource {
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }

    fn set_snapshot_frequency_by_item(&mut self, item_interval: u64) {
        self.snapshot_generator.set_item_interval(item_interval);
    }

    fn set_snapshot_frequency_by_time(&mut self, time_interval: Duration) {
        self.snapshot_generator.set_time_interval(time_interval);
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct FileSourceState {
    current: u64,
    terminated: bool,
}

impl Operator<String> for FileSource {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        let global_id = metadata.global_id;
        let instances = metadata.replicas.len();

        let file = File::open(&self.path).unwrap_or_else(|err| {
            panic!(
                "FileSource: error while opening file {:?}: {:?}",
                self.path, err
            )
        });
        let file_size = file.metadata().unwrap().len() as usize;

        let range_size = file_size / instances;
        let start = range_size * global_id as usize;
        self.current = start;
        self.end = if global_id as usize == instances - 1 {
            file_size
        } else {
            start + range_size
        };

        let mut reader = BufReader::new(file);
        // Seek reader to the first byte to be read
        reader
            .seek(SeekFrom::Current(start as i64))
            .expect("seek file");
        if global_id != 0 {
            // discard first line
            let mut v = Vec::new();

            self.current += reader
                .read_until(b'\n', &mut v)
                .expect("Cannot read line from file");
        }
        self.coord = Some(metadata.coord);
        self.reader = Some(reader);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;


        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
    }

    fn next(&mut self) -> StreamElement<String> {
        if self.terminated {
            log::debug!("{} emitting terminate", self.coord.unwrap());
            return StreamElement::Terminate;
        }
        // Check snapshot generator
        let snapshot = self.snapshot_generator.get_snapshot_marker();
        if snapshot.is_some() {
            let snapshot_id = snapshot.unwrap();
            // Save state and forward snapshot marker
            let state = FileSourceState{
                current: self.current as u64,
                terminated: self.terminated,
            }; 
            self.persistency_service.save_state(self.operator_coord, snapshot_id, state);
            return StreamElement::Snapshot(snapshot_id);
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
            coord: None,
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
