#!/usr/bin/env python3  
"""  
Production-ready BlockImageUpdate implementation for Python 3.13  
Optimized for transfer.list V4 command parsing and execution  
"""  
  
import sys  
import os  
import subprocess  
import struct  
import hashlib  
import tempfile  
import threading  
import time  
import queue  
import mmap  
import ctypes  
import bsdiff4
from typing import Self, List, Dict, Any, Optional, BinaryIO, Iterator, Tuple, Union  
from pathlib import Path  
import json  
from contextlib import suppress, contextmanager  
from dataclasses import dataclass  
import logging  

# Configure logging  
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')  
logger = logging.getLogger(__name__)  

try:  
    from ApplyPatch import applypatch, apply_bsdiff_patch, apply_imgdiff_patch
    APPLYPATCH_AVAILABLE = True  
except ImportError:  
    APPLYPATCH_AVAILABLE = False  
    logger.warning("ApplyPatch module not available, will use external process")
  
@dataclass  
class IOSettings:  
    """Platform-specific I/O optimization settings"""  
    buffer_size: int  
    use_direct_io: bool  
    sync_method: str  
  
class RangeSet:  
    """Optimized RangeSet implementation with better validation"""  
      
    __slots__ = ('pos', 'count', 'size')  
      
    def __init__(self, range_text: str):  
        if not range_text or not isinstance(range_text, str):  
            raise ValueError("Range text must be a non-empty string")  
          
        pieces = range_text.split(',')  
        if len(pieces) < 3:  
            raise ValueError(f"Invalid range format: {range_text}")  
          
        try:  
            num = int(pieces[0])  
        except ValueError:  
            raise ValueError(f"Invalid range count: {pieces[0]}")  
          
        if num == 0:  
            raise ValueError("Range count cannot be zero")  
        if num % 2 != 0:  
            raise ValueError("Range count must be even (pairs of start,end)")  
        if num != len(pieces) - 1:  
            raise ValueError(f"Range count mismatch: expected {num}, got {len(pieces) - 1}")  
          
        self.pos = []  
        self.count = num // 2  
        self.size = 0  
          
        # Parse and validate ranges  
        for i in range(0, num, 2):  
            try:  
                start = int(pieces[i + 1])  
                end = int(pieces[i + 2])  
            except (ValueError, IndexError) as e:  
                raise ValueError(f"Invalid range values at position {i}: {e}")  
              
            if start < 0 or end < 0:  
                raise ValueError(f"Range values cannot be negative: {start}, {end}")  
            if start >= end:  
                raise ValueError(f"Invalid range: start >= end ({start} >= {end})")  
              
            self.pos.extend([start, end])  
            self.size += end - start  
          
        # Validate ranges don't overlap  
        self._validate_no_overlaps()  
      
    def _validate_no_overlaps(self):  
        """Ensure ranges don't overlap"""  
        ranges = [(self.pos[i], self.pos[i + 1]) for i in range(0, len(self.pos), 2)]  
        ranges.sort()  
          
        for i in range(len(ranges) - 1):  
            if ranges[i][1] > ranges[i + 1][0]:  
                raise ValueError(f"Overlapping ranges: [{ranges[i][0]}-{ranges[i][1]}) and [{ranges[i + 1][0]}-{ranges[i + 1][1]})")  
      
    def __iter__(self):  
        """Iterate over (start, end) pairs"""  
        for i in range(0, len(self.pos), 2):  
            yield (self.pos[i], self.pos[i + 1])  
      
    def __len__(self) -> int:  
        return self.count  
      
    def get_ranges(self) -> List[Tuple[int, int]]:  
        """Return list of (start, end) tuples"""  
        return [(self.pos[i], self.pos[i + 1]) for i in range(0, len(self.pos), 2)]  
  
class RangeSinkState:  
    """Python implementation of C++ RangeSinkState for precise range writing"""  
      
    def __init__(self, rangeset: RangeSet, fd: BinaryIO, blocksize: int = 4096):  
        self.tgt = rangeset  
        self.fd = fd  
        self.blocksize = blocksize  
        self.p_block = 0  
        self.p_remain = 0  
        self._setup_first_range()  
      
    def _setup_first_range(self):  
        """Setup the first range for writing"""  
        if self.tgt.count > 0:  
            self.p_remain = (self.tgt.pos[1] - self.tgt.pos[0]) * self.blocksize  
            offset = self.tgt.pos[0] * self.blocksize  
            self.fd.seek(offset)  
      
    def write(self, data: bytes) -> int:  
        """Write data using RangeSink logic, returns bytes written"""  
        if self.p_remain == 0:  
            logger.warning("Range sink write overrun")  
            return 0  
          
        written = 0  
        data_view = memoryview(data)  
          
        while len(data_view) > 0 and self.p_remain > 0:  
            write_now = min(len(data_view), self.p_remain)  
              
            try:  
                actual_written = self.fd.write(data_view[:write_now])  
                if actual_written != write_now:  
                    logger.error(f"Short write: expected {write_now}, got {actual_written}")  
                    break  
            except OSError as e:  
                logger.error(f"Write failed: {e}")  
                break  
              
            data_view = data_view[write_now:]  
            self.p_remain -= write_now  
            written += write_now  
              
            if self.p_remain == 0:  
                # Move to next block  
                self.p_block += 1  
                if self.p_block < self.tgt.count:  
                    self.p_remain = (self.tgt.pos[self.p_block * 2 + 1] -   
                                   self.tgt.pos[self.p_block * 2]) * self.blocksize  
                    offset = self.tgt.pos[self.p_block * 2] * self.blocksize  
                    self.fd.seek(offset)  
                else:  
                    break  
          
        return written  
  
class BlockImageUpdate:  
    """Production-ready BlockImageUpdate implementation for Python 3.13"""  
      
    BLOCKSIZE = 4096  
      
    def __init__(self, blockdev_path: str, transfer_list_path: str, new_data_path: str, patch_data_path: str, continue_on_error: bool = False):  

        self.blockdev_path = Path(blockdev_path)  
        self.transfer_list_path = Path(transfer_list_path)  
        self.new_data_path = Path(new_data_path)  
        self.patch_data_path = Path(patch_data_path)  
          
        # Core state  
        self.stash_map: Dict[str, bytes] = {}  
        self.written = 0  
        self.version = 1  
        self.total_blocks = 0  
        self.transfer_lines: List[str] = []  
          
        # File descriptors  
        self.new_data_fd: Optional[BinaryIO] = None  
        self.patch_data_fd: Optional[BinaryIO] = None  
        self.patch_data_mmap: Optional[mmap.mmap] = None  
          
        # Platform-specific setup  
        self.io_settings = self._get_optimal_io_settings()  
        self.stash_base_dir = self._get_stash_directory()  
          
        # Threading for new data streaming  
        self.new_data_queue: queue.Queue[Optional[bytes]] = queue.Queue(maxsize=20)  
        self.new_data_producer_thread: Optional[threading.Thread] = None  
        self.new_data_producer_running = threading.Event()  
        self.new_data_condition = threading.Condition()  
          
        # Progress tracking  
        self._last_progress_time = time.time()  
        self.patch_stream_empty = False  

        # FUCKING ERRORs
        self.continue_on_error = continue_on_error
      
    def _get_optimal_io_settings(self) -> IOSettings:  
        """Get platform-specific optimal I/O settings"""  
        if os.name == 'nt':  # Windows  
            return IOSettings(  
                buffer_size=64 * 1024,  # 64KB for Windows  
                use_direct_io=False,  
                sync_method='flush'  
            )  
        else:  # Unix-like  
            return IOSettings(  
                buffer_size=1024 * 1024,  # 1MB for Unix  
                use_direct_io=True,  
                sync_method='fsync'  
            )  
      
    def _get_stash_directory(self) -> Path:  
        """Get platform-appropriate stash directory"""  
        if os.name == 'nt':  
            return Path(os.environ.get('TEMP', 'C:\\temp')) / 'biu_cache'  
        else:  
            return Path('/tmp/biu_cache')  
      
    @contextmanager  
    def _open_block_device(self, mode='rb'):  
        """Context manager for opening block device with proper error handling"""  
        try:  
            if os.name == 'nt':  
                if self.blockdev_path.as_posix().startswith('\\\\.\\'):  
                    fd = self.blockdev_path.open(mode, buffering=0)  
                else:  
                    fd = self.blockdev_path.open(mode)  
            else:  
                fd = self.blockdev_path.open(mode)  
              
            try:  
                yield fd  
            finally:  
                fd.close()  
                  
        except PermissionError:  
            logger.error(f"Permission denied accessing {self.blockdev_path}")  
            logger.info("Note: Block device access may require administrator/root privileges")  
            raise  
      
    def _new_data_producer(self):  
        """Optimized producer thread with better buffering"""  
        logger.info("New data producer thread starting")  
        buffer_size = self.BLOCKSIZE * 10  # Read multiple blocks at once  
        
        should_exit = False  
        
        try:  
            while self.new_data_producer_running.is_set() and not should_exit:  
                try:  
                    # Read larger chunks for better I/O efficiency  
                    data = self.new_data_fd.read(buffer_size)  
                    if not data:  
                        self.new_data_queue.put(None)  # EOF signal  
                        should_exit = True  
                        continue  
                    
                    # Split into individual blocks  
                    for i in range(0, len(data), self.BLOCKSIZE):  
                        if not self.new_data_producer_running.is_set():  
                            should_exit = True  
                            break  
                        
                        block = data[i:i + self.BLOCKSIZE]  
                        if len(block) < self.BLOCKSIZE:  
                            block = block.ljust(self.BLOCKSIZE, b'\x00')  
                        
                        self.new_data_queue.put(block)  
                        
                        with self.new_data_condition:  
                            self.new_data_condition.notify_all()  
                
                except* (IOError, OSError) as eg:  
                    for error in eg.exceptions:  
                        logger.error(f"Producer error: {error}")  
                    self.new_data_queue.put(None)  
                    should_exit = True  # 设置标志变量而不是return  
                    
        except Exception as e:  
            logger.error(f"Producer fatal error: {e}")  
        finally:  
            logger.info("New data producer thread terminating")

      
    def __enter__(self) -> Self:  
        """Enhanced context manager entry"""  
        logger.info("Initializing BlockImageUpdate")  
          
        # Platform-specific warnings  
        if os.name == 'nt':  
            logger.warning("Running on Windows - some operations may require administrator privileges")  
          
        # Check patch stream  
        if self.patch_data_path.exists() and self.patch_data_path.stat().st_size == 0:  
            self.patch_stream_empty = True  
            logger.info("Patch stream is empty, will skip erase and zero commands")  
            self._create_empty_block_device_file()  
          
        try:  
            # Open new data file with memory mapping if possible  
            if self.new_data_path.exists():  
                logger.info(f"Opening new data file: {self.new_data_path}")  
                self.new_data_fd = self.new_data_path.open('rb')  
                logger.info(f"New data file size: {self.new_data_path.stat().st_size} bytes")  
                  
                # Start producer thread  
                self.new_data_producer_running.set()  
                self.new_data_producer_thread = threading.Thread(  
                    target=self._new_data_producer,  
                    daemon=True  
                )  
                self.new_data_producer_thread.start()  
                  
                # Verify thread started  
                time.sleep(0.1)  
                if not self.new_data_producer_thread.is_alive():  
                    raise RuntimeError("Failed to start new data producer thread")  
                logger.info("New data producer thread started successfully")  
              
            # Open patch data with memory mapping for better performance  
            if self.patch_data_path.exists():  
                self.patch_data_fd = self.patch_data_path.open('rb')  
                try:  
                    self.patch_data_mmap = mmap.mmap(  
                        self.patch_data_fd.fileno(), 0, access=mmap.ACCESS_READ  
                    )  
                    logger.info("Patch data memory-mapped successfully")  
                except (OSError, ValueError):  
                    logger.warning("Failed to memory-map patch data, using regular file I/O")  
              
            # Create stash directory  
            self.stash_base_dir.mkdir(parents=True, exist_ok=True)  
              
        except Exception as e:  
            logger.error(f"Failed to initialize: {e}")  
            raise  
          
        return self  
    def __exit__(self, exc_type, exc_val, exc_tb):  
        """Enhanced cleanup with proper resource management"""  
        logger.info("Cleaning up BlockImageUpdate resources")  
          
        # Stop producer thread  
        if self.new_data_producer_thread and self.new_data_producer_thread.is_alive():  
            self.new_data_producer_running.clear()  
            with self.new_data_condition:  
                self.new_data_condition.notify_all()  
            self.new_data_producer_thread.join(timeout=5)  
          
        # Close file descriptors and memory maps  
        with suppress(AttributeError, OSError):  
            if self.patch_data_mmap:  
                self.patch_data_mmap.close()  
                self.patch_data_mmap = None  
          
        with suppress(AttributeError, OSError):  
            if self.new_data_fd:  
                self.new_data_fd.close()  
                self.new_data_fd = None  
          
        with suppress(AttributeError, OSError):  
            if self.patch_data_fd:  
                self.patch_data_fd.close()  
                self.patch_data_fd = None  
          
        # Clean up progress checkpoint on normal completion  
        if exc_type is None:  
            self._cleanup_progress_checkpoint()  
          
        # Cleanup stash  
        self._cleanup_stash()  
          
        # Clear queue  
        with suppress(Exception):  
            while not self.new_data_queue.empty():  
                try:  
                    self.new_data_queue.get_nowait()  
                    self.new_data_queue.task_done()  
                except queue.Empty:  
                    break  
  
    def _create_empty_block_device_file(self):  
        """Create empty block device file when patch.dat is empty"""  
        try:  
            if self.blockdev_path.exists():  
                logger.info(f"Removing existing block device file: {self.blockdev_path}")  
                self.blockdev_path.unlink()  
              
            logger.info(f"Creating empty block device file: {self.blockdev_path}")  
            self.blockdev_path.touch()  
            logger.info("Successfully created empty block device file")  
                  
        except Exception as e:  
            logger.error(f"Failed to create empty block device file: {e}")  
            raise  
  
    def _cleanup_stash(self):  
        """Clean up stash files and directory"""  
        try:  
            if self.stash_base_dir.exists():  
                for file_path in self.stash_base_dir.iterdir():  
                    file_path.unlink()  
                self.stash_base_dir.rmdir()  
                logger.info("Stash directory cleaned up")  
        except Exception as e:  
            logger.warning(f"Failed to cleanup stash: {e}")  
  
    def _cleanup_progress_checkpoint(self):  
        """Clean up progress checkpoint file after successful completion"""  
        try:  
            progress_file = self.stash_base_dir / "progress.json"  
            if progress_file.exists():  
                progress_file.unlink()  
                logger.info("Progress checkpoint cleaned up")  
        except Exception as e:  
            logger.warning(f"Failed to cleanup progress checkpoint: {e}")  
  
    def perform_command_zero(self, tokens: List[str], pos: int) -> int:  
        """Zero out specified block ranges with enhanced error handling"""  
        if pos >= len(tokens):  
            logger.error("Missing target blocks for zero")  
            return -1  
              
        try:  
            rangeset = RangeSet(tokens[pos])  
            logger.info(f"Zeroing {rangeset.size} blocks")  
              
            zero_block = b'\x00' * self.BLOCKSIZE  
              
            with self._open_block_device('r+b') as f:  
                for start_block, end_block in rangeset:  
                    offset = start_block * self.BLOCKSIZE  
                    f.seek(offset)  
                      
                    for block in range(start_block, end_block):  
                        f.write(zero_block)  
                      
                    # Sync based on platform  
                    if self.io_settings.sync_method == 'fsync':  
                        os.fsync(f.fileno())  
                    else:  
                        f.flush()  
              
            self.written += rangeset.size  
            return 0  
              
        except Exception as e:  
            logger.error(f"Failed to zero blocks: {e}")  
            return -1  
  
    def perform_command_new(self, tokens: List[str], pos: int) -> int:  
        """Write new data to specified blocks using RangeSink"""  
        if pos >= len(tokens):  
            logger.error("Missing target blocks for new")  
            return -1  
  
        try:  
            rangeset = RangeSet(tokens[pos])  
            logger.info(f"Writing {rangeset.size} blocks of new data")  
  
            # Auto-expand block device if needed  
            max_block_needed = max(end for _, end in rangeset)  
            required_device_size = max_block_needed * self.BLOCKSIZE  
              
            current_device_size = self.blockdev_path.stat().st_size  
            if current_device_size < required_device_size:  
                logger.info(f"Auto-expanding block device from {current_device_size} to {required_device_size} bytes")  
                with self._open_block_device('r+b') as f:  
                    f.seek(0, 2)  
                    padding_needed = required_device_size - current_device_size  
                    f.write(b'\x00' * padding_needed)  
                    f.flush()  
  
            # Check producer thread  
            if not self.new_data_producer_thread or not self.new_data_producer_thread.is_alive():  
                logger.error("New data producer thread is not running")  
                return -1  
  
            with self._open_block_device('r+b') as f:  
                range_sink = RangeSinkState(rangeset, f, self.BLOCKSIZE)  
                  
                blocks_to_write = rangeset.size  
                for _ in range(blocks_to_write):  
                    try:  
                        data = self.new_data_queue.get(timeout=60)  
                        if data is None:  # EOF signal  
                            logger.warning("Unexpected EOF from new data stream")  
                            break  
                          
                        if len(data) != self.BLOCKSIZE:  
                            data = data.ljust(self.BLOCKSIZE, b'\x00')  
                          
                        written = range_sink.write(data)  
                        if written != self.BLOCKSIZE:  
                            logger.error(f"Short write: expected {self.BLOCKSIZE}, got {written}")  
                            return -1  
                              
                        self.new_data_queue.task_done()  
                          
                    except queue.Empty:  
                        logger.error("Timeout waiting for new data")  
                        return -1  
  
            self.written += rangeset.size  
            return 0  
  
        except Exception as e:  
            logger.error(f"Failed to write new data: {e}")  
            return -1  
  
    def perform_command_move(self, tokens: List[str], pos: int) -> int:  
        """Move data between block ranges with proper version handling"""  
        try:  
            if self.version == 1:  
                tgt_rangeset, src_data, _ = self._load_src_tgt_version1(tokens, pos)  
            elif self.version == 2:  
                tgt_rangeset, src_data, _, _ = self._load_src_tgt_version2(tokens, pos)  
            else:  
                tgt_rangeset, src_data, _, _ = self._load_src_tgt_version3(tokens, pos, onehash=True)  
        except Exception as e:  
            logger.error(f"Failed to parse move command: {e}")  
            return -1  
  
        # Trim src_data to target size  
        expected_size = tgt_rangeset.size * self.BLOCKSIZE  
        if len(src_data) < expected_size:  
            logger.error(f"Source data too short: src={len(src_data)}, tgt={expected_size}")  
            return -1  
        elif len(src_data) > expected_size:  
            logger.warning(f"Source data too long, trimming: src={len(src_data)}, tgt={expected_size}")  
            src_data = src_data[:expected_size]  
  
        logger.info(f"Moving {tgt_rangeset.size} blocks")  
          
        try:  
            if not self._write_blocks(tgt_rangeset, src_data):  
                return -1  
            self.written += tgt_rangeset.size  
            return 0  
        except Exception as e:  
            logger.error(f"Failed to move blocks: {e}")  
            return -1  
  
    def perform_command_diff(self, tokens: List[str], pos: int, cmd_name: str) -> int:  
        """Apply diff patches with memory-mapped patch data"""  
        if pos + 1 >= len(tokens):  
            logger.error(f"Missing patch offset or length for {cmd_name}")  
            return -1  
  
        try:  
            offset = int(tokens[pos])  
            length = int(tokens[pos + 1])  
            pos += 2  
  
            # Parse based on transfer list version  
            if self.version >= 3:  
                if pos + 4 >= len(tokens):  
                    logger.error("Missing required parameters for version 3+ diff")  
                    return -1  
                  
                src_hash = tokens[pos]  
                tgt_hash = tokens[pos + 1]  
                tgt_range = tokens[pos + 2]  
                src_blocks = int(tokens[pos + 3])  
                pos += 4  
  
                tgt_rangeset = RangeSet(tgt_range)  
                  
                # Handle source data  
                if pos < len(tokens) and tokens[pos] != "-":  
                    src_rangeset = RangeSet(tokens[pos])  
                    src_data = self._read_blocks(src_rangeset)  
                    pos += 1  
                else:  
                    src_data = b'\x00' * (src_blocks * self.BLOCKSIZE)  
                    if pos < len(tokens):  
                        pos += 1  
  
                # Handle stash specifications  
                while pos < len(tokens):  
                    stash_spec = tokens[pos]  
                    if ':' in stash_spec:  
                        self._apply_stash_data(src_data, stash_spec)  
                    pos += 1  
            else:  
                # Version 1/2 handling  
                tgt_rangeset = RangeSet(tokens[pos])  
                src_rangeset = RangeSet(tokens[pos + 1])  
                src_data = self._read_blocks(src_rangeset)  
                tgt_hash = None  
  
            # Verify source hash if available  
            if self.version >= 3 and src_hash and src_hash != "0" * 40:  
                actual_src_hash = hashlib.sha1(src_data).hexdigest()  
                if actual_src_hash != src_hash:  
                    logger.error(f"Source hash mismatch: expected {src_hash}, got {actual_src_hash}")  
                    return -1  
  
            # Extract patch data using memory mapping if available  
            if self.patch_data_mmap:  
                patch_data = self.patch_data_mmap[offset:offset + length]  
            else:  
                self.patch_data_fd.seek(offset)  
                patch_data = self.patch_data_fd.read(length)  
  
            # Apply patch  
            result = self._apply_patch_internal(src_data, patch_data, cmd_name, tgt_hash)
            if result is None:  
                return -1  
  
            # Write result using RangeSink  
            with self._open_block_device('r+b') as f:  
                range_sink = RangeSinkState(tgt_rangeset, f, self.BLOCKSIZE)  
                  
                data_offset = 0  
                while data_offset < len(result):  
                    chunk = result[data_offset:data_offset + self.BLOCKSIZE]  
                    if len(chunk) < self.BLOCKSIZE:  
                        chunk = chunk.ljust(self.BLOCKSIZE, b'\x00')  
                      
                    written = range_sink.write(chunk)  
                    if written == 0:  
                        break  
                    data_offset += written  
  
            self.written += tgt_rangeset.size  
            return 0  
  
        except Exception as e:  
            logger.error(f"Failed to apply {cmd_name} patch: {e}")  
            return -1  
  
    def _apply_patch_internal(self, src_data: bytes, patch_data: bytes,   
                            cmd_name: str, expected_tgt_hash: str) -> Optional[bytes]:  
        """Apply patch using imported ApplyPatch functions with error handling"""  
        if not APPLYPATCH_AVAILABLE:  
            return self._apply_patch_external(src_data, patch_data, cmd_name, expected_tgt_hash)  
        
        try:  
            if patch_data.startswith(b'BSDIFF40'):  
                result = apply_bsdiff_patch(src_data, patch_data)  
            elif patch_data.startswith(b'IMGDIFF2'):  
                result = apply_imgdiff_patch(src_data, patch_data)  
            else:  
                logger.error("Unknown patch format")  
                return None  
    
            # Verify target hash if provided  
            if expected_tgt_hash and expected_tgt_hash != "0" * 40:  
                actual_hash = hashlib.sha1(result).hexdigest()  
                if actual_hash != expected_tgt_hash:  
                    logger.error(f"Target hash mismatch: expected {expected_tgt_hash}, got {actual_hash}")  
                    return None  
    
            return result  
    
        except (OverflowError, ValueError) as e:  
            logger.warning(f"Integer overflow in patch application: {e}, falling back to external process")  
            return self._apply_patch_external(src_data, patch_data, cmd_name, expected_tgt_hash)  
        except Exception as e:  
            logger.error(f"Failed to apply patch using imported functions: {e}")  
            return self._apply_patch_external(src_data, patch_data, cmd_name, expected_tgt_hash)

    def _apply_patch_external(self, src_data: bytes, patch_data: bytes,   
                            cmd_name: str, expected_tgt_hash: str) -> Optional[bytes]:  
        """Apply patch using external ApplyPatch.py"""  
        try:  
            with tempfile.TemporaryDirectory() as temp_dir:  
                temp_dir_path = Path(temp_dir)  
                  
                # Create temporary files  
                temp_src = temp_dir_path / "source.dat"  
                temp_patch = temp_dir_path / "patch.dat"  
                temp_tgt = temp_dir_path / "target.dat"  
                  
                temp_src.write_bytes(src_data)  
                temp_patch.write_bytes(patch_data)  
                  
                # Calculate source SHA1 and estimate target size  
                src_sha1 = hashlib.sha1(src_data).hexdigest()  
                target_size = self._estimate_target_size(patch_data, len(src_data))  
                  
                # Build command  
                cmd = [  
                    sys.executable, 'ApplyPatch.py',  
                    str(temp_src), str(temp_tgt),  
                    expected_tgt_hash, str(target_size),  
                    src_sha1, str(temp_patch)  
                ]  
                  
                # Execute with timeout  
                result = subprocess.run(  
                    cmd, capture_output=True, text=True,   
                    timeout=300, cwd=Path.cwd()  
                )  
                  
                if result.returncode != 0:  
                    logger.error(f"ApplyPatch.py failed: {result.stderr}")  
                    return None  
                  
                # Read result  
                return temp_tgt.read_bytes()  
                  
        except Exception as e:  
            logger.error(f"Failed to apply patch externally: {e}")  
            return None  
  
    def _estimate_target_size(self, patch_data: bytes, src_data_len: int) -> int:  
        """Estimate target file size based on patch header"""  
        if len(patch_data) < 8:  
            return src_data_len  
          
        header = patch_data[:8]  
          
        if header == b"BSDIFF40":  
            # BSDiff format: target size at offset 24-32  
            if len(patch_data) >= 32:  
                try:  
                    return struct.unpack('<Q', patch_data[24:32])[0]  
                except struct.error:  
                    pass  
        elif header == b"IMGDIFF2":  
            # ImgDiff format: estimate based on source size  
            return src_data_len  
          
        return src_data_len  
  
    def perform_command_stash(self, tokens: List[str], pos: int) -> int:  
        """Stash blocks for later use with enhanced error handling"""  
        if pos + 1 >= len(tokens):  
            logger.error("Missing stash id or range for stash")  
            return -1  
              
        try:  
            stash_id = tokens[pos]  
            rangeset = RangeSet(tokens[pos + 1])  
              
            logger.info(f"Stashing {rangeset.size} blocks as {stash_id}")  
              
            # Read blocks to stash  
            stash_data = self._read_blocks(rangeset)  
            if stash_data is None:  
                return -1  
              
            # Store in memory and on disk  
            self.stash_map[stash_id] = stash_data  
              
            # Save to disk for persistence  
            stash_file = self.stash_base_dir / f"stash_{stash_id}"  
            stash_file.write_bytes(stash_data)  
              
            return 0  
              
        except Exception as e:  
            logger.error(f"Failed to stash blocks: {e}")  
            return -1  
  
    def perform_command_free(self, tokens: List[str], pos: int) -> int:  
        """Free stashed data with enhanced cleanup"""  
        if pos >= len(tokens):  
            logger.error("Missing stash id for free")  
            return -1  
              
        stash_id = tokens[pos]  
          
        # Remove from memory  
        if stash_id in self.stash_map:  
            del self.stash_map[stash_id]  
            logger.info(f"Freed stash {stash_id}")  
        else:  
            logger.warning(f"Stash {stash_id} not found (already freed?)")  
          
        # Remove from disk  
        stash_file = self.stash_base_dir / f"stash_{stash_id}"  
        with suppress(FileNotFoundError):  
            stash_file.unlink()  
          
        return 0  
  
    def _read_blocks(self, rangeset: RangeSet) -> Optional[bytes]:  
        """Read blocks from block device with streaming optimization"""  
        try:  
            data = bytearray()  
              
            with self._open_block_device('rb') as f:  
                for start_block, end_block in rangeset:  
                    f.seek(start_block * self.BLOCKSIZE)  
                      
                    # Read in chunks for better performance  
                    remaining_blocks = end_block - start_block  
                    while remaining_blocks > 0:  
                        chunk_blocks = min(remaining_blocks, 256)  # 1MB chunks  
                        chunk_size = chunk_blocks * self.BLOCKSIZE  
                          
                        chunk_data = f.read(chunk_size)  
                        if len(chunk_data) != chunk_size:  
                            logger.error(f"Short read: expected {chunk_size}, got {len(chunk_data)}")  
                            return None  
                          
                        data.extend(chunk_data)  
                        remaining_blocks -= chunk_blocks  
              
            return bytes(data)  
              
        except Exception as e:  
            logger.error(f"Failed to read blocks: {e}")  
            return None  
  
    def _write_blocks(self, rangeset: RangeSet, data: bytes) -> bool:  
        """Write data to blocks using RangeSink for precision"""  
        try:  
            with self._open_block_device('r+b') as f:  
                range_sink = RangeSinkState(rangeset, f, self.BLOCKSIZE)  
                  
                data_offset = 0  
                while data_offset < len(data):  
                    chunk = data[data_offset:data_offset + self.BLOCKSIZE]  
                    if len(chunk) < self.BLOCKSIZE:  
                        chunk = chunk.ljust(self.BLOCKSIZE, b'\x00')  
                      
                    written = range_sink.write(chunk)  
                    if written == 0:  
                        break  
                    data_offset += written  
                  
                # Sync based on platform settings  
                if self.io_settings.sync_method == 'fsync':  
                    os.fsync(f.fileno())  
                else:  
                    f.flush()  
              
            return True  
              
        except Exception as e:  
            logger.error(f"Failed to write blocks: {e}")  
            return False  
  
    def _load_src_tgt_version1(self, tokens: List[str], pos: int) -> Tuple[RangeSet, bytes, int]:  
        """Load source/target for version 1 commands"""  
        if pos + 1 >= len(tokens):  
            raise ValueError("Invalid parameters for version 1 load")  
          
        src_rangeset = RangeSet(tokens[pos])  
        tgt_rangeset = RangeSet(tokens[pos + 1])  
        src_data = self._read_blocks(src_rangeset)  
          
        if src_data is None:  
            raise IOError("Failed to read source blocks for version 1")  
          
        return tgt_rangeset, src_data, src_rangeset.size  
  
    def _load_src_tgt_version2(self, tokens: List[str], pos: int) -> Tuple[RangeSet, bytearray, int, bool]:  
        """Load source/target for version 2 commands"""  
        if pos + 2 >= len(tokens):  
            raise ValueError("Invalid parameters for version 2 load")  
    
        tgt_rangeset = RangeSet(tokens[pos])  
        src_block_count = int(tokens[pos + 1])  
        
        # Initialize buffer with exact expected size  
        src_data = bytearray(src_block_count * self.BLOCKSIZE)  
        overlap = False  
        cur = pos + 2  
    
        if tokens[cur] == "-":  
            cur += 1  # No source range, only stashes  
        else:  
            src_rangeset = RangeSet(tokens[cur])  
            read_data = self._read_blocks(src_rangeset)  
            if read_data is None:  
                raise IOError("Failed to read source blocks for version 2")  
            
            copy_size = min(len(read_data), len(src_data))  
            src_data[:copy_size] = read_data[:copy_size]  
            
            if len(read_data) > len(src_data):  
                logger.warning(f"Truncating read data from {len(read_data)} to {len(src_data)} bytes to match src_block_count")  
            
            overlap = self._range_overlaps(src_rangeset, tgt_rangeset)  
            cur += 1  
            
            # Optional source location mapping  
            if cur < len(tokens) and ':' not in tokens[cur]:  
                locs_rangeset = RangeSet(tokens[cur])  
                self._move_range(src_data, locs_rangeset, src_data[:copy_size])  
                cur += 1  
    
        # CRITICAL FIX: Handle stashes and update src_data reference  
        while cur < len(tokens):  
            stash_spec = tokens[cur]  
            if ':' in stash_spec:  
                # Apply stash and update src_data with the returned buffer  
                src_data = self._apply_stash_data(src_data, stash_spec)  
                # Ensure src_data is still the correct type and size  
                if not isinstance(src_data, bytearray):  
                    src_data = bytearray(src_data)  
                if len(src_data) != src_block_count * self.BLOCKSIZE:  
                    logger.warning(f"Stash application changed buffer size from {src_block_count * self.BLOCKSIZE} to {len(src_data)}")  
            cur += 1  
    
        return tgt_rangeset, src_data, src_block_count, overlap
    
    def _load_src_tgt_version3(self, tokens: List[str], pos: int, onehash: bool) -> Tuple[RangeSet, bytearray, int, bool]:  
        """  
        Parse and load source/target for version 3+ commands.  
        Returns: (tgt_rangeset, src_data, src_block_count, overlap)  
        """  
        if pos >= len(tokens):  
            raise ValueError("Missing source hash for version 3+ load")  
    
        src_hash = tokens[pos]  
        pos += 1  
        if onehash:  
            tgt_hash = src_hash  
        else:  
            if pos >= len(tokens):  
                raise ValueError("Missing target hash for version 3+ load")  
            tgt_hash = tokens[pos]  
            pos += 1  
    
        # Call version 2 loader to get base data and overlap status  
        tgt_rangeset, src_data, src_block_count, overlap = self._load_src_tgt_version2(tokens, pos)  
    
        # Verify source hash - src_data should now be exactly src_block_count * BLOCKSIZE  
        actual_src_hash = hashlib.sha1(src_data).hexdigest()  
        if actual_src_hash != src_hash:  
            if overlap:  
                # Try to recover from memory or disk stash  
                if src_hash in self.stash_map:  
                    recovered_data = self.stash_map[src_hash]  
                    # Ensure recovered data has the expected size  
                    if len(recovered_data) != src_block_count * self.BLOCKSIZE:  
                        raise ValueError(f"Recovered stash data size mismatch: expected {src_block_count * self.BLOCKSIZE}, got {len(recovered_data)}")  
                    src_data = bytearray(recovered_data)  
                    
                    if hashlib.sha1(src_data).hexdigest() != src_hash:  
                        raise ValueError("Partition has unexpected contents and stash recovery failed")  
                else:  
                    # Try to load from disk  
                    stash_file = self.stash_base_dir / f"stash_{src_hash}"  
                    if stash_file.exists():  
                        recovered_data = stash_file.read_bytes()  
                        if len(recovered_data) != src_block_count * self.BLOCKSIZE:  
                            raise ValueError(f"Recovered disk stash data size mismatch: expected {src_block_count * self.BLOCKSIZE}, got {len(recovered_data)}")  
                        src_data = bytearray(recovered_data)  
                        
                        if hashlib.sha1(src_data).hexdigest() != src_hash:  
                            raise ValueError("Partition has unexpected contents and disk stash recovery failed")  
                    else:  
                        raise ValueError("Partition has unexpected contents")  
            else:  
                raise ValueError("Partition has unexpected contents")  
        else:  
            if overlap:  
                # Proactively write stash to disk  
                self.stash_map[src_hash] = bytes(src_data)  
                stash_file = self.stash_base_dir / f"stash_{src_hash}"  
                stash_file.write_bytes(src_data)  
    
        return tgt_rangeset, src_data, src_block_count, overlap
  
    def _apply_stash_data(self, buffer: Union[bytearray, bytes], stash_spec: str):  
        """Apply stashed data to buffer at specified ranges"""  
        try:  
            stash_id, range_text = stash_spec.split(':', 1)  
            
            if stash_id not in self.stash_map:  
                # Try loading from disk  
                stash_file = self.stash_base_dir / f"stash_{stash_id}"  
                if stash_file.exists():  
                    stash_data = stash_file.read_bytes()  
                    self.stash_map[stash_id] = stash_data  
                else:  
                    logger.warning(f"Stash ID {stash_id} not found")  
                    return buffer if isinstance(buffer, bytearray) else bytearray(buffer)  
    
            stash_data = self.stash_map[stash_id]  
            locs = RangeSet(range_text)  
    
            # CRITICAL FIX: Ensure buffer is mutable (bytearray)  
            if isinstance(buffer, bytes):  
                buffer = bytearray(buffer)  
            elif not isinstance(buffer, bytearray):  
                buffer = bytearray(buffer)  
    
            current_offset = len(stash_data)  
            for start_block, end_block in reversed(locs.get_ranges()):  
                num_blocks = end_block - start_block  
                size_to_copy = num_blocks * self.BLOCKSIZE  
                
                current_offset -= size_to_copy  
                if current_offset < 0:  
                    raise ValueError(f"Stash data too small for range {start_block}-{end_block}")  
    
                buffer_start = start_block * self.BLOCKSIZE  
                buffer_end = buffer_start + size_to_copy  
                
                if buffer_end > len(buffer):  
                    raise ValueError(f"Write beyond buffer bounds: {buffer_end} > {len(buffer)}")  
    
                # Now safe to do slice assignment on bytearray  
                buffer[buffer_start:buffer_end] = stash_data[current_offset:current_offset + size_to_copy]  
                logger.debug(f"Applied stash {stash_id} to blocks {start_block}-{end_block}")  
    
            return buffer  
    
        except Exception as e:  
            logger.error(f"Failed to apply stash {stash_spec}: {e}")  
            raise


    def _move_range(self, dest: bytearray, locs: RangeSet, source: bytes):  
        """Move packed source data to locations in dest buffer"""  
        if len(source) != locs.size * self.BLOCKSIZE:  
            raise ValueError(f"Source data size mismatch: expected {locs.size * self.BLOCKSIZE}, got {len(source)}")  
          
        current_offset = len(source)  
        for start_block, end_block in reversed(locs.get_ranges()):  
            num_blocks = end_block - start_block  
            size_to_copy = num_blocks * self.BLOCKSIZE  
              
            current_offset -= size_to_copy  
            if current_offset < 0:  
                raise ValueError(f"Source data too small for range {start_block}-{end_block}")  
  
            dest_start = start_block * self.BLOCKSIZE  
            dest_end = dest_start + size_to_copy  
  
            if dest_end > len(dest):  
                raise ValueError(f"Write beyond destination bounds: {dest_end} > {len(dest)}")  
              
            dest[dest_start:dest_end] = source[current_offset:current_offset + size_to_copy]  
  
    def _range_overlaps(self, r1: RangeSet, r2: RangeSet) -> bool:  
        """Check if two RangeSet objects have any overlapping blocks"""  
        ranges1 = sorted(r1.get_ranges())  
        ranges2 = sorted(r2.get_ranges())  
  
        i, j = 0, 0  
        while i < len(ranges1) and j < len(ranges2):  
            start1, end1 = ranges1[i]  
            start2, end2 = ranges2[j]  
  
            # Check for overlap  
            if max(start1, start2) < min(end1, end2):  
                return True  # Overlap found  
  
            # Move to the next range in the set that ends earlier  
            if end1 < end2:  
                i += 1  
            else:  
                j += 1  
        return False  
  
    def process_transfer_list(self) -> int:  
        """Process the transfer list file with resume capability"""  
        try:  
            with self.transfer_list_path.open('r', encoding='utf-8') as f:  
                self.transfer_lines = [line.strip() for line in f]  
        except (IOError, UnicodeDecodeError) as e:  
            logger.error(f"Failed to read transfer list: {e}")  
            return -1  
  
        # Parse header  
        if len(self.transfer_lines) < 2:  
            logger.error("Transfer list too short")  
            return -1  
  
        try:  
            self.version = int(self.transfer_lines[0])  
            self.total_blocks = int(self.transfer_lines[1])  
        except ValueError as e:  
            logger.error(f"Failed to parse transfer list header: {e}")  
            return -1  
  
        logger.info(f"blockimg version is {self.version}")  
        logger.info(f"total blocks: {self.total_blocks}")  
  
        if self.total_blocks == 0:  
            return 0  
  
        # Handle version-specific header parsing  
        if self.version >= 2:  
            if len(self.transfer_lines) < 4:  
                logger.error("Transfer list too short for version 2+")  
                return -1  
            max_stash_entries = self.transfer_lines[2]  
            max_stash_blocks = int(self.transfer_lines[3])  
            logger.info(f"maximum stash entries {max_stash_entries}")  
            logger.info(f"maximum stash blocks {max_stash_blocks}")  
  
        # Determine resume point  
        start_line = self._determine_resume_point()  
  
        # Process commands  
        result = self._process_commands(start_line)  
  
        # Clean up on successful completion  
        if result == 0:  
            self._cleanup_progress_checkpoint()  
            logger.info("Successfully processed commands")  
              
            if not self.patch_stream_empty:  
                logger.info(f"wrote {self.written} blocks; expected {self.total_blocks}")  
  
        return result  
  
    def _determine_resume_point(self) -> int:  
        """Determine resume point using combined checkpoint and hash verification"""  
        start_line = 4 if self.version >= 2 else 2  
  
        # Try checkpoint first  
        resume_line = self._load_progress_checkpoint()  
        if resume_line > start_line:  
            logger.info(f"Resuming from checkpoint at line {resume_line}")  
            return resume_line  
  
        # Fallback to hash verification  
        resume_line = self._quick_resume_check()  
        if resume_line > start_line:  
            logger.info(f"Resuming from hash verification at line {resume_line}")  
            return resume_line  
  
        return start_line  
  
    def _load_progress_checkpoint(self) -> int:  
        """Load progress checkpoint and return line number to resume from"""  
        try:  
            progress_file = self.stash_base_dir / "progress.json"  
            if not progress_file.exists():  
                return -1  
  
            checkpoint = json.loads(progress_file.read_text())  
  
            # Verify checkpoint is for same transfer list version  
            if checkpoint.get('version') != self.version:  
                logger.warning("Checkpoint version mismatch, starting from beginning")  
                return -1  
  
            self.written = checkpoint['written']  
            logger.info(f"Resuming from line {checkpoint['line']}, {self.written} blocks written")  
            return checkpoint['line']  
  
        except Exception as e:  
            logger.error(f"Failed to load checkpoint: {e}")  
            return -1  
  
    def _quick_resume_check(self) -> int:  
        """Quick resume check using target hash verification"""  
        start_line = 4 if self.version >= 2 else 2  
  
        for line_num in range(start_line, len(self.transfer_lines)):  
            line = self.transfer_lines[line_num].strip()  
            if not line:  
                continue  
  
            tokens = line.split()  
            if not tokens:  
                continue  
  
            cmd = tokens[0]  
  
            # Only check commands that modify data  
            if cmd in ['bsdiff', 'imgdiff', 'new', 'move']:  
                if not self._is_command_completed(tokens, cmd):  
                    return line_num  
  
        return -1  # All commands completed  
  
    def _is_command_completed(self, tokens: List[str], cmd_name: str) -> bool:  
        """Check if a specific command has been completed"""  
        try:  
            if self.version >= 3 and cmd_name in ['bsdiff', 'imgdiff']:  
                if len(tokens) >= 6:  
                    tgt_hash = tokens[4]  
                    tgt_range = tokens[5]  
  
                    tgt_rangeset = RangeSet(tgt_range)  
                    actual_data = self._read_blocks(tgt_rangeset)  
                    if actual_data:  
                        actual_hash = hashlib.sha1(actual_data).hexdigest()  
                        return actual_hash == tgt_hash  
            elif cmd_name == 'new':  
                if len(tokens) >= 2:  
                    tgt_range = tokens[1]  
                    tgt_rangeset = RangeSet(tgt_range)  
                    actual_data = self._read_blocks(tgt_rangeset)  
                    if actual_data:  
                        # Check if blocks are not all zeros  
                        return not all(b == 0 for b in actual_data)  
  
        except Exception as e:  
            logger.error(f"Error checking command completion: {e}")  
  
        return False  
  
    def _process_commands(self, start_line: int) -> int:  
        """Process transfer list commands starting from given line"""  
        commands = {  
            'zero': lambda tokens, pos: self._handle_zero_command(tokens, pos),  
            'new': lambda tokens, pos: self.perform_command_new(tokens, pos),  
            'erase': lambda tokens, pos: self._handle_erase_command(tokens, pos),  
            'move': lambda tokens, pos: self.perform_command_move(tokens, pos),  
            'bsdiff': lambda tokens, pos: self.perform_command_diff(tokens, pos, 'bsdiff'),  
            'imgdiff': lambda tokens, pos: self.perform_command_diff(tokens, pos, 'imgdiff'),  
            'stash': lambda tokens, pos: self.perform_command_stash(tokens, pos),  
            'free': lambda tokens, pos: self.perform_command_free(tokens, pos),  
        }  
    
        failed_commands = 0  # 记录失败的命令数量  
    
        for line_num in range(start_line, len(self.transfer_lines)):  
            line = self.transfer_lines[line_num].strip()  
            if not line:  
                continue  
    
            tokens = line.split()  
            if not tokens:  
                continue  
    
            cmd = tokens[0]  
            if cmd not in commands:  
                logger.error(f"Unknown command: {cmd}")  
                if not self.continue_on_error:  
                    return -1  
                failed_commands += 1  
                continue  
    
            logger.info(f"Executing: {cmd}")  
            try:  
                result = commands[cmd](tokens, 1)  
                if result != 0:  
                    logger.error(f"Command {cmd} failed with code {result}")  
                    if self.continue_on_error:  
                        logger.warning(f"Continuing execution despite error in {cmd} command")  
                        failed_commands += 1  
                        continue  
                    else:  
                        return result  
    
                # Update progress and save checkpoint if needed  
                self._update_and_save_progress(line_num, cmd)  
    
            except Exception as e:  
                logger.error(f"Exception executing command {cmd}: {e}")  
                if self.continue_on_error:  
                    logger.warning(f"Continuing execution despite exception in {cmd} command")  
                    failed_commands += 1  
                    continue  
                else:  
                    return -1  
    
        # 返回失败命令的数量，0表示全部成功  
        return failed_commands

  
    def _handle_zero_command(self, tokens: List[str], pos: int) -> int:  
        """Handle zero command with patch stream check"""  
        if self.patch_stream_empty:  
            logger.info("Skipping zero command...")  
            return 0  
        return self.perform_command_zero(tokens, pos)  
  
    def _handle_erase_command(self, tokens: List[str], pos: int) -> int:  
        """Handle erase command with patch stream check"""  
        if self.patch_stream_empty:  
            logger.info("Skipping erase command...")  
            return 0  
        return self.perform_command_zero(tokens, pos)  # Erase -> zero for compatibility  
  
    def _update_and_save_progress(self, line_num: int, cmd: str):  
        """Update progress and save checkpoint if needed"""  
        if self._should_save_checkpoint(line_num, cmd):  
            self._save_progress_checkpoint(line_num + 1, self.written)  
  
        # Update progress display  
        if self.total_blocks > 0:  
            progress = (self.written * 100.0) / self.total_blocks  
            logger.info(f"Progress: {progress:.1f}% ({self.written}/{self.total_blocks} blocks)")  
  
    def _should_save_checkpoint(self, line_num: int, cmd: str) -> bool:  
        """Determine if checkpoint should be saved at this point"""  
        return line_num % 10 == 0 or cmd in ['bsdiff', 'imgdiff', 'new']  
  
    def _save_progress_checkpoint(self, line_num: int, written_blocks: int):  
        """Save progress checkpoint to JSON file"""  
        checkpoint = {  
            'line': line_num,  
            'written': written_blocks,  
            'timestamp': time.time(),  
            'version': self.version  
        }  
        progress_file = self.stash_base_dir / "progress.json"  
        progress_file.write_text(json.dumps(checkpoint, indent=2))  
  
  
def main():  
    """Main entry point"""  
    if len(sys.argv) < 5:  
        print(f"usage: {sys.argv[0]} <system.img> <system.transfer.list> <system.new.dat> <system.patch.dat> [--continue-on-error]")  
        print("args:")  
        print("\t- block device (or file) to modify in-place")  
        print("\t- transfer list (blob)")  
        print("\t- new data stream (filename within package.zip)")  
        print("\t- patch stream (filename within package.zip, must be uncompressed)")  
        print("\t--continue-on-error: continue execution even if commands fail")  
        return 1  
  
    # 检查是否有continue-on-error参数  
    continue_on_error = '--continue-on-error' in sys.argv  
  
    try:  
        with BlockImageUpdate(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],   
                             continue_on_error=continue_on_error) as biu:  
            result = biu.process_transfer_list()  
  
            if result == 0:  
                print("Done")  
            elif continue_on_error and result > 0:  
                print(f"Done with {result} failed commands (continued execution)")  
            else:  
                print(f"Done with error code: {result}")  
  
            return result  
    except Exception as e:  
        print(f"Fatal error: {e}")  
        return -1

  
if __name__ == "__main__":  
    sys.exit(main())
    