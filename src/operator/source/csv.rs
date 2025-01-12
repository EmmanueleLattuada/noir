use std::fmt::Display;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::PathBuf;

use csv::{Reader, ReaderBuilder, Terminator, Trim};
use serde::{Deserialize, Serialize};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::network::OperatorCoord;
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::Stream;

use super::SnapshotGenerator;

/// Wrapper that limits the bytes that can be read from a type that implements `io::Read`.
struct LimitedReader<R: Read> {
    inner: R,
    /// Bytes remaining to be read.
    remaining: usize,
}

impl<R: Read> LimitedReader<R> {
    fn new(inner: R, remaining: usize) -> Self {
        Self { inner, remaining }
    }
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_bytes = if self.remaining > 0 {
            // if there are some bytes to be read, call read on the inner reader
            self.inner.read(buf)?.min(self.remaining)
        } else {
            // all the bytes have been read
            0
        };
        self.remaining -= read_bytes;
        Ok(read_bytes)
    }
}

/// Options for the CSV parser.
#[derive(Clone)]
struct CsvOptions {
    /// Byte used to mark a line as a comment.
    comment: Option<u8>,
    /// Field delimiter.
    delimiter: u8,
    /// Whether quotes are escaped by using doubled quotes.
    double_quote: bool,
    /// Byte used to escape quotes.
    escape: Option<u8>,
    /// Whether to allow records with different number of fields.
    flexible: bool,
    /// Byte used to quote fields.
    quote: u8,
    /// Whether to enable field quoting.
    quoting: bool,
    /// Line terminator.
    terminator: Terminator,
    /// Whether to trim fields and/or headers.
    trim: Trim,
    /// Whether the CSV file has headers.
    has_headers: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            comment: None,
            delimiter: b',',
            double_quote: true,
            escape: None,
            flexible: false,
            quote: b'"',
            quoting: true,
            terminator: Terminator::CRLF,
            trim: Trim::None,
            has_headers: true,
        }
    }
}

/// Source that reads and parses a CSV file.
///
/// The file is divided in chunks and is read concurrently by multiple replicas.
pub struct CsvSource<Out: Data + for<'a> Deserialize<'a>> {
    /// Path of the file.
    path: PathBuf,
    /// Reader used to parse the CSV file.
    csv_reader: Option<Reader<LimitedReader<BufReader<File>>>>,
    /// Options to customize the CSV parser.
    options: CsvOptions,
    /// Whether the reader has terminated its job.
    terminated: bool,
    /// Operator coordinate
    operator_coord: OperatorCoord,
    /// Snapshot markers generator
    snapshot_generator: SnapshotGenerator,
    /// Persistency service to save the state
    persistency_service: Option<PersistencyService<CsvSourceState>>,
    snapshot_before_flush: bool,
    _out: PhantomData<Out>,
}

impl<Out: Data + for<'a> Deserialize<'a>> Display for CsvSource<Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CsvSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> CsvSource<Out> {
    /// Create a new source that reads and parse the lines of a CSV file.
    ///
    /// The file is partitioned into as many chunks as replicas, each replica has to have the
    /// **same** file in the same path. It is guaranteed that each line of the file is emitted by
    /// exactly one replica.
    ///
    /// After creating the source it's possible to customize its behaviour using one of the
    /// available methods. By default it is assumed that the delimiter is `,` and the CSV has
    /// headers.
    ///
    /// Each line will be deserialized into the type `Out`, so the structure of the CSV must be
    /// valid for that deserialization. The [`csv`](https://crates.io/crates/csv) crate is used for
    /// the parsing.
    ///
    /// **Note**: the file must be readable and its size must be available. This means that only
    /// regular files can be read.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::CsvSource;
    /// # use serde::{Deserialize, Serialize};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// #[derive(Clone, Deserialize, Serialize)]
    /// struct Thing {
    ///     what: String,
    ///     count: u64,
    /// }
    /// let source = CsvSource::<Thing>::new("/datasets/huge.csv");
    /// let s = env.stream(source);
    /// ```
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            csv_reader: None,
            options: Default::default(),
            terminated: false,
            // This is the first operator in the chain so operator_id = 0
            // Other fields will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
            snapshot_generator: SnapshotGenerator::new(),
            snapshot_before_flush: false,
            persistency_service: None,
            _out: PhantomData,
        }
    }

    /// The comment character to use when parsing CSV.
    ///
    /// If the start of a record begins with the byte given here, then that line is ignored by the
    /// CSV parser.
    ///
    /// This is disabled by default.
    pub fn comment(mut self, comment: Option<u8>) -> Self {
        self.options.comment = comment;
        self
    }

    /// The field delimiter to use when parsing CSV.
    ///
    /// The default is `,`.
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.options.delimiter = delimiter;
        self
    }

    /// Enable double quote escapes.
    ///
    /// This is enabled by default, but it may be disabled. When disabled, doubled quotes are not
    /// interpreted as escapes.
    pub fn double_quote(mut self, double_quote: bool) -> Self {
        self.options.double_quote = double_quote;
        self
    }

    /// The escape character to use when parsing CSV.
    ///
    /// In some variants of CSV, quotes are escaped using a special escape character like `\`
    /// (instead of escaping quotes by doubling them).
    ///
    /// By default, recognizing these idiosyncratic escapes is disabled.
    pub fn escape(mut self, escape: Option<u8>) -> Self {
        self.options.escape = escape;
        self
    }

    /// Whether the number of fields in records is allowed to change or not.
    ///
    /// When disabled (which is the default), parsing CSV data will return an error if a record is
    /// found with a number of fields different from the number of fields in a previous record.
    ///
    /// When enabled, this error checking is turned off.
    pub fn flexible(mut self, flexible: bool) -> Self {
        self.options.flexible = flexible;
        self
    }

    /// The quote character to use when parsing CSV.
    ///
    /// The default is `"`.
    pub fn quote(mut self, quote: u8) -> Self {
        self.options.quote = quote;
        self
    }

    /// Enable or disable quoting.
    ///
    /// This is enabled by default, but it may be disabled. When disabled, quotes are not treated
    /// specially.
    pub fn quoting(mut self, quoting: bool) -> Self {
        self.options.quoting = quoting;
        self
    }

    /// The record terminator to use when parsing CSV.
    ///
    /// A record terminator can be any single byte. The default is a special value,
    /// `Terminator::CRLF`, which treats any occurrence of `\r`, `\n` or `\r\n` as a single record
    /// terminator.
    pub fn terminator(mut self, terminator: Terminator) -> Self {
        self.options.terminator = terminator;
        self
    }

    /// Whether fields are trimmed of leading and trailing whitespace or not.
    ///
    /// By default, no trimming is performed. This method permits one to override that behavior and
    /// choose one of the following options:
    ///
    /// 1. `Trim::Headers` trims only header values.
    /// 2. `Trim::Fields` trims only non-header or "field" values.
    /// 3. `Trim::All` trims both header and non-header values.
    ///
    /// A value is only interpreted as a header value if this CSV reader is configured to read a
    /// header record (which is the default).
    ///
    /// When reading string records, characters meeting the definition of Unicode whitespace are
    /// trimmed. When reading byte records, characters meeting the definition of ASCII whitespace
    /// are trimmed. ASCII whitespace characters correspond to the set `[\t\n\v\f\r ]`.
    pub fn trim(mut self, trim: Trim) -> Self {
        self.options.trim = trim;
        self
    }

    /// Whether to treat the first row as a special header row.
    ///
    /// By default, the first row is treated as a special header row, which means the header is
    /// never returned by any of the record reading methods or iterators. When this is disabled
    /// (`yes` set to `false`), the first row is not treated specially.
    ///
    /// Note that the `headers` and `byte_headers` methods are unaffected by whether this is set.
    /// Those methods always return the first record.
    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.options.has_headers = has_headers;
        self
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> Source<Out> for CsvSource<Out> {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct CsvSourceState{
    current: u64,
    snapshot_before_flush: bool,
}

impl<Out: Data + for<'a> Deserialize<'a>> Operator<Out> for CsvSource<Out> {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        let global_id = metadata.global_id;
        let instances = metadata.replicas.len();

        self.operator_coord.setup_coord(metadata.coord);
        let mut last_position = None;
        if let Some(pb) = metadata.persistency_builder{
            let p_service = pb.generate_persistency_service::<CsvSourceState>();
            let mut set_snap_gen =  true;
            if metadata.contains_iterative_oper {
                set_snap_gen = !metadata.iterations_snapshot_alignment;
                self.snapshot_before_flush = true;
            }
            if set_snap_gen {
                if let Some(snap_freq) = p_service.snapshot_frequency_by_item {
                    self.snapshot_generator.set_item_interval(snap_freq);
                }
                if let Some(snap_freq) = p_service.snapshot_frequency_by_time {
                    self.snapshot_generator.set_time_interval(snap_freq);
                }
            }
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if let Some(snap_id) = snapshot_id {
                // Get the persisted state
                let opt_state: Option<CsvSourceState> = p_service.get_state(self.operator_coord, snap_id.clone());
                if let Some(state) = opt_state {
                    self.terminated = snap_id.terminate();
                    last_position = Some(state.current);
                    // Don't send twice the "snapshot before flush"
                    self.snapshot_before_flush = state.snapshot_before_flush;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
                self.snapshot_generator.restart_from(snap_id);
            }
            self.persistency_service = Some(p_service);
        }

        let file = File::options()
            .read(true)
            .write(false)
            .open(&self.path)
            .unwrap_or_else(|err| {
                panic!(
                    "CsvSource: error while opening file {:?}: {:?}",
                    self.path, err
                )
            });

        let file_size = file.metadata().unwrap().len();

        let mut buf_reader = BufReader::new(file);

        let last_byte_terminator = match self.options.terminator {
            Terminator::CRLF => b'\n',
            Terminator::Any(terminator) => terminator,
            _ => unreachable!(),
        };

        // Handle the header
        let mut header = Vec::new();
        let header_size = if self.options.has_headers {
            buf_reader
                .read_until(last_byte_terminator, &mut header)
                .expect("Error while reading CSV header") as u64
        } else {
            0
        };

        // Calculate start and end offset of this replica
        let body_size = file_size - header_size;
        let range_size = body_size / instances as u64;
        let mut start = header_size + range_size * global_id;
        let mut end = if global_id as usize == instances - 1 {
            file_size
        } else {
            start + range_size
        };

        // Set start to last position (from persisted state), if any
        if let Some(last_p) = last_position {
            start = last_p;
        }

        // Align start byte
        if global_id != 0 {
            // Seek reader to the first byte to be read
            buf_reader
                .seek(SeekFrom::Start(start))
                .expect("Error while seeking BufReader to start");
            // discard first line
            let mut buf = Vec::new();
            start += buf_reader
                .read_until(last_byte_terminator, &mut buf)
                .expect("Error while reading first line from file") as u64;
        }

        // Align end byte
        if global_id as usize != instances - 1 {
            // Seek reader to the last byte to be read
            buf_reader
                .seek(SeekFrom::Start(end))
                .expect("Error while seeking BufReader to end");
            // get to the end of the line
            let mut buf = Vec::new();
            end += buf_reader
                .read_until(last_byte_terminator, &mut buf)
                .expect("Error while reading last line from file") as u64;
        }

        // Rewind BufReader to the start
        buf_reader
            .seek(SeekFrom::Start(start))
            .expect("Error while rewinding BufReader");

        // Limit the number of bytes to be read
        let limited_reader = LimitedReader::new(buf_reader, (end - start) as usize);

        // Create csv::Reader
        let mut csv_reader = ReaderBuilder::new()
            .comment(self.options.comment)
            .delimiter(self.options.delimiter)
            .double_quote(self.options.double_quote)
            .escape(self.options.escape)
            .flexible(self.options.flexible)
            .quote(self.options.quote)
            .quoting(self.options.quoting)
            .terminator(self.options.terminator)
            .trim(self.options.trim)
            .has_headers(self.options.has_headers)
            .from_reader(limited_reader);

        if self.options.has_headers {
            // set the headers of the CSV file
            csv_reader.set_byte_headers(
                Reader::from_reader(header.as_slice())
                    .byte_headers()
                    .unwrap()
                    .to_owned(),
            );
        }

        self.csv_reader = Some(csv_reader);

    }

    fn next(&mut self) -> StreamElement<Out> {
        let csv_reader = self
            .csv_reader
            .as_mut()
            .expect("CsvSource was not initialized");

        if self.terminated {
            if self.persistency_service.is_some(){
                 // Save terminated state
                 let state = CsvSourceState {
                    current: csv_reader.position().byte(),
                    snapshot_before_flush: false,
                };         
                self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
            }
            return StreamElement::Terminate;
        }
        if self.persistency_service.is_some(){
            // Check snapshot generator
            let snapshot = self.snapshot_generator.get_snapshot_marker();
            if let Some(snapshot_id) = snapshot {
                // Save state and forward snapshot marker
                let state = CsvSourceState {
                    current: csv_reader.position().byte(),
                    snapshot_before_flush: self.snapshot_before_flush,
                };         
                self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id.clone(), state);
                return StreamElement::Snapshot(snapshot_id);
            }
        }

        match csv_reader.deserialize::<Out>().next() {
            Some(item) => StreamElement::Item(item.unwrap()),
            None => {
                if self.snapshot_before_flush {
                    self.snapshot_before_flush = false;
                    let snap_id = self.snapshot_generator.get_flush_snapshot_marker();
                    let state = CsvSourceState {
                        current: csv_reader.position().byte(),
                        snapshot_before_flush: false,
                    };
                    self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snap_id.clone(), state);
                    StreamElement::Snapshot(snap_id)
                } else {
                    self.terminated = true;
                    StreamElement::FlushAndRestart
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("CSVSource");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }

    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        // This operator is stateful
        vec![self.operator_coord.operator_id]
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> Clone for CsvSource<Out> {
    fn clone(&self) -> Self {
        assert!(
            self.csv_reader.is_none(),
            "CsvSource must be cloned before calling setup"
        );
        Self {
            path: self.path.clone(),
            csv_reader: None,
            options: self.options.clone(),
            terminated: false,
            operator_coord: self.operator_coord,
            snapshot_generator: self.snapshot_generator.clone(),
            snapshot_before_flush: self.snapshot_before_flush,
            persistency_service: self.persistency_service.clone(),
            _out: PhantomData,
        }
    }
}

impl crate::StreamEnvironment {
    /// Convenience method, creates a `CsvSource` and makes a stream using `StreamEnvironment::stream`
    pub fn stream_csv<T: Data + for<'a> Deserialize<'a>>(
        &mut self,
        path: impl Into<PathBuf>,
    ) -> Stream<T, CsvSource<T>> {
        let source = CsvSource::new(path);
        self.stream(source)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use tempfile::NamedTempFile;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source::CsvSource;

    #[test]
    fn csv_without_headers() {
        for num_records in 0..100 {
            for terminator in &["\n", "\r\n"] {
                let file = NamedTempFile::new().unwrap();
                for i in 0..num_records {
                    write!(file.as_file(), "{},{}{}", i, i + 1, terminator).unwrap();
                }

                let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
                let source = CsvSource::<(i32, i32)>::new(file.path()).has_headers(false);
                let res = env.stream(source).shuffle().collect_vec();
                env.execute_blocking();

                let mut res = res.get().unwrap();
                res.sort_unstable();
                assert_eq!(res, (0..num_records).map(|x| (x, x + 1)).collect_vec());
            }
        }
    }

    #[test]
    fn csv_with_headers() {
        #[derive(Clone, Serialize, Deserialize)]
        struct T {
            a: i32,
            b: i32,
        }

        for num_records in 0..100 {
            for terminator in &["\n", "\r\n"] {
                let file = NamedTempFile::new().unwrap();
                write!(file.as_file(), "a,b{terminator}").unwrap();
                for i in 0..num_records {
                    write!(file.as_file(), "{},{}{}", i, i + 1, terminator).unwrap();
                }

                let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
                let source = CsvSource::<T>::new(file.path());
                let res = env.stream(source).shuffle().collect_vec();
                env.execute_blocking();

                let res = res
                    .get()
                    .unwrap()
                    .into_iter()
                    .map(|x| (x.a, x.b))
                    .sorted()
                    .collect_vec();
                assert_eq!(res, (0..num_records).map(|x| (x, x + 1)).collect_vec());
            }
        }
    }
}
