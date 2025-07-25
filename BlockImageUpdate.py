#!/usr/bin/env python3  
"""  
Production-ready BlockImageUpdate implementation for Python 3.13  
Optimized for transfer.list V4 command parsing and execution  
"""  
  
import asyncio
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
import bsdiff4 # type: ignore
from typing import Self, List, Dict, Any, Optional, BinaryIO, Iterator, Tuple, Union, AsyncIterator  
from pathlib import Path  
import json  
from contextlib import suppress, contextmanager, redirect_stderr
from dataclasses import dataclass  
import logging
from functools import lru_cache  
from collections import OrderedDict
import weakref
import psutil  # 用于内存监控  
import gc      # 垃圾回收控制  

@dataclass  
class LargeFileConfig:  
    """Large file processing configuration"""  
    chunk_size_mb: int = 64          # 块大小（MB）  
    memory_limit_mb: int = 1024      # 内存限制（MB）  
    enable_streaming: bool = True     # 启用流式处理  
    gc_frequency: int = 10           # 垃圾回收频率  
    use_mmap_threshold_mb: int = 100 # 使用mmap的文件大小阈值

# Configure logging  
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')  
logger = logging.getLogger(__name__)  

try:  
    from ApplyPatch import apply_bsdiff_patch,apply_imgdiff_patch_streaming
    APPLYPATCH_AVAILABLE = True  
except ImportError:  
    APPLYPATCH_AVAILABLE = False  
    logger.warning("ApplyPatch module not available, will use external process")
  
class LRUStashCache:  
    """Thread-safe LRU cache for stash data with size limits"""  
      
    def __init__(self, max_items: int = 50, max_memory_mb: int = 500):  
        self.max_items = max_items  
        self.max_memory_bytes = max_memory_mb * 1024 * 1024  
        self.cache = OrderedDict()  
        self.current_memory = 0  
        self.lock = threading.RLock()  
        self.disk_cache_dir = None  
      
    def set_disk_cache_dir(self, cache_dir: Path):  
        """Set directory for disk-based cache fallback"""  
        self.disk_cache_dir = cache_dir  
          
    def get(self, key: str) -> Optional[bytes]:  
        """Get stash data, checking memory cache first, then disk"""  
        with self.lock:  
            # Check memory cache first  
            if key in self.cache:  
                # Move to end (most recently used)  
                value = self.cache.pop(key)  
                self.cache[key] = value  
                return value  
              
            # Check disk cache  
            if self.disk_cache_dir:  
                disk_file = self.disk_cache_dir / f"stash_{key}"  
                if disk_file.exists():  
                    try:  
                        data = disk_file.read_bytes()  
                        # Add to memory cache if there's room  
                        self._add_to_memory_cache(key, data)  
                        return data  
                    except Exception as e:  
                        logger.warning(f"Failed to read disk cache for {key}: {e}")  
              
            return None  
      
    def put(self, key: str, value: bytes):  
        """Store stash data with automatic eviction"""  
        with self.lock:  
            # Remove if already exists to update position  
            if key in self.cache:  
                old_value = self.cache.pop(key)  
                self.current_memory -= len(old_value)  
              
            # Try to add to memory cache  
            if self._can_fit_in_memory(value):  
                self._add_to_memory_cache(key, value)  
            else:  
                # Store to disk if too large for memory  
                self._store_to_disk(key, value)  
      
    def _can_fit_in_memory(self, value: bytes) -> bool:  
        """Check if value can fit in memory cache"""  
        return len(value) <= self.max_memory_bytes // 4  # Don't use more than 25% for single item  
      
    def _add_to_memory_cache(self, key: str, value: bytes):  
        """Add item to memory cache with eviction"""  
        value_size = len(value)  
          
        # Evict items if necessary  
        while (len(self.cache) >= self.max_items or   
               self.current_memory + value_size > self.max_memory_bytes):  
            if not self.cache:  
                break  
            self._evict_oldest()  
          
        # Add new item  
        self.cache[key] = value  
        self.current_memory += value_size  
      
    def _evict_oldest(self):  
        """Evict the oldest item from memory cache"""  
        if not self.cache:  
            return  
              
        oldest_key, oldest_value = self.cache.popitem(last=False)  
        self.current_memory -= len(oldest_value)  
          
        # Store evicted item to disk if possible  
        self._store_to_disk(oldest_key, oldest_value)  
      
    def _store_to_disk(self, key: str, value: bytes):  
        """Store item to disk cache"""  
        if not self.disk_cache_dir:  
            return  
              
        try:  
            disk_file = self.disk_cache_dir / f"stash_{key}"  
            disk_file.write_bytes(value)  
            logger.debug(f"Stored stash {key} to disk ({len(value)} bytes)")  
        except Exception as e:  
            logger.warning(f"Failed to store stash {key} to disk: {e}")  
      
    def remove(self, key: str):  
        """Remove item from both memory and disk cache"""  
        with self.lock:  
            # Remove from memory  
            if key in self.cache:  
                value = self.cache.pop(key)  
                self.current_memory -= len(value)  
              
            # Remove from disk  
            if self.disk_cache_dir:  
                disk_file = self.disk_cache_dir / f"stash_{key}"  
                try:  
                    if disk_file.exists():  
                        disk_file.unlink()  
                except Exception as e:  
                    logger.warning(f"Failed to remove disk cache for {key}: {e}")  
      
    def clear(self):  
        """Clear all cached data"""  
        with self.lock:  
            self.cache.clear()  
            self.current_memory = 0  
              
            # Clear disk cache  
            if self.disk_cache_dir and self.disk_cache_dir.exists():  
                try:  
                    for file_path in self.disk_cache_dir.glob("stash_*"):  
                        file_path.unlink()  
                except Exception as e:  
                    logger.warning(f"Failed to clear disk cache: {e}")  
      
    def get_memory_usage(self) -> dict:  
        """Get current memory usage statistics"""  
        with self.lock:  
            return {  
                'items_in_memory': len(self.cache),  
                'memory_bytes': self.current_memory,  
                'memory_mb': self.current_memory / (1024 * 1024),  
                'max_items': self.max_items,  
                'max_memory_mb': self.max_memory_bytes / (1024 * 1024)  
            }

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
  

class AsyncBlockProcessor:  
    """Async block processor for improved I/O performance"""  
      
    def __init__(self, blocksize: int = 4096, max_concurrent: int = 4):  
        self.blocksize = blocksize  
        self.max_concurrent = max_concurrent  
        self.semaphore = asyncio.Semaphore(max_concurrent)  
      
    async def read_blocks_async(self, fd: BinaryIO, rangeset: RangeSet) -> bytes:  
        """Asynchronously read blocks from file descriptor"""  
        tasks = []  
          
        for start_block, end_block in rangeset:  
            task = self._read_range_async(fd, start_block, end_block)  
            tasks.append(task)  
          
        results = await asyncio.gather(*tasks)  
        return b''.join(results)  
      
    async def _read_range_async(self, fd: BinaryIO, start_block: int, end_block: int) -> bytes:  
        """Read a single range asynchronously"""  
        async with self.semaphore:  
            loop = asyncio.get_event_loop()  
              
            def read_range():  
                offset = start_block * self.blocksize  
                size = (end_block - start_block) * self.blocksize  
                fd.seek(offset)  
                return fd.read(size)  
              
            return await loop.run_in_executor(None, read_range)  
      
    async def write_blocks_async(self, fd: BinaryIO, rangeset: RangeSet, data: bytes) -> bool:  
        """Asynchronously write blocks to file descriptor"""  
        tasks = []  
        data_offset = 0  
          
        for start_block, end_block in rangeset:  
            chunk_size = (end_block - start_block) * self.blocksize  
            chunk_data = data[data_offset:data_offset + chunk_size]  
              
            task = self._write_range_async(fd, start_block, chunk_data)  
            tasks.append(task)  
            data_offset += chunk_size  
          
        results = await asyncio.gather(*tasks)  
        return all(results)  
      
    async def _write_range_async(self, fd: BinaryIO, start_block: int, data: bytes) -> bool:  
        """Write a single range asynchronously"""  
        async with self.semaphore:  
            loop = asyncio.get_event_loop()  
              
            def write_range():  
                try:  
                    offset = start_block * self.blocksize  
                    fd.seek(offset)  
                    fd.write(data)  
                    return True  
                except Exception as e:  
                    logger.error(f"Async write failed: {e}")  
                    return False  
              
            return await loop.run_in_executor(None, write_range)


class BlockImageUpdate:  
    """Production-ready BlockImageUpdate implementation for Python 3.13"""  
      
    BLOCKSIZE = 4096  
      
    def __init__(self, blockdev_path: str, transfer_list_path: str, new_data_path: str, patch_data_path: str, continue_on_error: bool = False):  
        self.blockdev_path = Path(blockdev_path)  
        self.transfer_list_path = Path(transfer_list_path)  
        self.new_data_path = Path(new_data_path)  
        self.patch_data_path = Path(patch_data_path)  
        
        # 检测文件大小并配置处理策略  
        self.large_file_config = self._configure_large_file_handling()  
        
        # Core state  
        self.stash_cache = LRUStashCache(  
            max_items=5 if self.large_file_config.enable_streaming else 10,  # Much smaller cache  
            max_memory_mb=min(64, self.large_file_config.memory_limit_mb // 4)  # Very limited memory  
        )
        self.written = 0  
        self.version = 1  
        self.total_blocks = 0  
        self.transfer_lines: List[str] = []  
        
        # File descriptors with streaming support  
        self.new_data_fd: Optional[BinaryIO] = None  
        self.patch_data_fd: Optional[BinaryIO] = None  
        self.patch_data_mmap: Optional[mmap.mmap] = None  
        
        # 添加文件访问锁和流式处理状态  
        self.new_data_lock = threading.RLock()  
        self.patch_data_lock = threading.RLock()  
        self.streaming_state = {  
            'current_chunk': 0,  
            'total_chunks': 0,  
            'memory_usage': 0  
        }  
        
        # Platform-specific setup  
        self.io_settings = self._get_optimal_io_settings()  
        self.stash_base_dir = self._get_stash_directory()  
        
        # Enhanced threading for large files  
        self.new_data_queue: queue.Queue[Optional[bytes]] = queue.Queue(  
            maxsize=max(2, min(4, self.large_file_config.chunk_size_mb // 8))  # Very small queue  
        )
        self.new_data_producer_thread: Optional[threading.Thread] = None  
        self.new_data_producer_running = threading.Event()  
        self.new_data_condition = threading.Condition()  
        
        # Progress tracking with memory monitoring  
        self._last_progress_time = time.time()  
        self._last_gc_time = time.time()  
        self.patch_stream_empty = False  
        
        # Error handling  
        self.continue_on_error = continue_on_error  
        self.failed_command_details = {}

    def _configure_large_file_handling(self) -> LargeFileConfig:  
        """Configure for extreme memory constraints (2GB RAM, 4-15GB files)"""  
        config = LargeFileConfig()  
        
        try:  
            available_memory_mb = psutil.virtual_memory().available // (1024 * 1024)  
            
            # Detect file sizes  
            file_sizes = {}  
            for name, path in [  
                ('new_data', self.new_data_path),  
                ('patch_data', self.patch_data_path),  
                ('blockdev', self.blockdev_path)  
            ]:  
                if path.exists():  
                    size_mb = path.stat().st_size // (1024 * 1024)  
                    file_sizes[name] = size_mb  
            
            max_file_size = max(file_sizes.values()) if file_sizes else 0  
            
            # Aggressive configuration for 2GB RAM constraint  
            if available_memory_mb < 3000:  # Less than 3GB available  
                config.chunk_size_mb = 4      # Very small chunks  
                config.memory_limit_mb = 256   # Use only 256MB max  
                config.enable_streaming = True  
                config.gc_frequency = 1        # Very aggressive GC  
                config.use_mmap_threshold_mb = 20  # Use mmap for smaller files only  
            elif max_file_size > 4096:  # >4GB files  
                config.chunk_size_mb = 8  
                config.memory_limit_mb = 512  
                config.enable_streaming = True  
                config.gc_frequency = 2  
            else:  
                config.enable_streaming = False  
            
            logger.info(f"Large file config for memory constraint: {config}")  
            return config  
            
        except Exception as e:  
            logger.warning(f"Failed to configure large file handling: {e}")  
            # Fallback to very conservative settings  
            config.chunk_size_mb = 4  
            config.memory_limit_mb = 256  
            config.enable_streaming = True  
            config.gc_frequency = 1  
            return config


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
        """Optimized producer thread with better buffering and error recovery"""  
        logger.info("New data producer thread starting")  
        buffer_size = self.BLOCKSIZE * 10  
        
        try:  
            while self.new_data_producer_running.is_set():  
                try:  
                    # Check if file descriptor is still valid  
                    if not self.new_data_fd or self.new_data_fd.closed:  
                        logger.error("New data file descriptor is closed")  
                        break  
                    
                    data = self.new_data_fd.read(buffer_size)  
                    if not data:  
                        self.new_data_queue.put(None)  # EOF signal  
                        break  
                    
                    # Split into individual blocks  
                    for i in range(0, len(data), self.BLOCKSIZE):  
                        if not self.new_data_producer_running.is_set():  
                            break  
                        
                        block = data[i:i + self.BLOCKSIZE]  
                        if len(block) < self.BLOCKSIZE:  
                            block = block.ljust(self.BLOCKSIZE, b'\x00')  
                        
                        # Use timeout to avoid blocking indefinitely  
                        try:  
                            self.new_data_queue.put(block, timeout=5)  
                        except queue.Full:  
                            logger.warning("New data queue is full, producer may be blocked")  
                            continue  
                        
                        with self.new_data_condition:  
                            self.new_data_condition.notify_all()  
                    
                except (IOError, OSError) as e:  
                    logger.error(f"Producer I/O error: {e}")  
                    break  
                except Exception as e:  
                    logger.error(f"Producer unexpected error: {e}")  
                    break  
                    
        except Exception as e:  
            logger.error(f"Producer fatal error: {e}")  
        finally:  
            # Signal EOF and cleanup  
            try:  
                self.new_data_queue.put(None)  
            except:  
                pass  
            logger.info("New data producer thread terminating")  
    
    def _restart_producer_thread(self) -> bool:    
        """Thread-safe restart of the producer thread"""    
        try:    
            # Stop existing thread if running    
            if self.new_data_producer_thread and self.new_data_producer_thread.is_alive():    
                self.new_data_producer_running.clear()    
                self.new_data_producer_thread.join(timeout=2)    
            
            # 使用锁保护文件状态检查  
            with self.new_data_lock:  
                # Check if new data file is still valid    
                if not self.new_data_fd or self.new_data_fd.closed:    
                    logger.error("New data file descriptor is not available for restart")    
                    return False    
                
                # 保持当前文件位置，不要重置    
                try:    
                    current_pos = self.new_data_fd.tell()    
                    self.new_data_fd.seek(0, 2)    
                    file_size = self.new_data_fd.tell()    
                    self.new_data_fd.seek(current_pos)    
                    
                    logger.info(f"Restarting producer at position: {current_pos}/{file_size}")    
                    
                    if current_pos >= file_size:    
                        logger.info("At end of file, producer may not need restart")    
                        return True  # 不是错误，只是到了文件末尾    
                        
                except Exception as e:    
                    logger.error(f"Cannot determine file position: {e}")    
                    return False    
            
            # Restart the thread    
            self.new_data_producer_running.set()    
            self.new_data_producer_thread = threading.Thread(    
                target=self._new_data_producer,    
                daemon=True    
            )    
            self.new_data_producer_thread.start()    
            
            # Verify thread started    
            time.sleep(0.1)    
            is_alive = self.new_data_producer_thread.is_alive()    
            
            if not is_alive:    
                logger.error("Producer thread failed to start or died immediately")    
            
            return is_alive    
            
        except Exception as e:    
            logger.error(f"Failed to restart producer thread: {e}")    
            return False

      
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
            # Create stash directory and set up cache  
            self.stash_base_dir.mkdir(parents=True, exist_ok=True)    
            self.stash_cache.set_disk_cache_dir(self.stash_base_dir)  
            
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
                
        except Exception as e:    
            logger.error(f"Failed to initialize: {e}")    
            raise    
            
        return self

  
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
        """Clean up stash files and directory with LRU cache cleanup"""  
        try:  
            # Clear the LRU cache first  
            self.stash_cache.clear()  
            logger.info("Stash cache cleared")  
            
            # Clean up any remaining files in stash directory  
            if self.stash_base_dir.exists():  
                for file_path in self.stash_base_dir.iterdir():  
                    try:  
                        file_path.unlink()  
                    except Exception as e:  
                        logger.warning(f"Failed to remove stash file {file_path}: {e}")  
                
                try:  
                    self.stash_base_dir.rmdir()  
                    logger.info("Stash directory cleaned up")  
                except OSError as e:  
                    logger.warning(f"Failed to remove stash directory: {e}")  
                    
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
        """Write new data with performance monitoring integration"""  
        if pos >= len(tokens):  
            logger.error("Missing target blocks for new")  
            return -1  
        
        try:  
            rangeset = RangeSet(tokens[pos])  
            logger.info(f"Writing {rangeset.size} blocks of new data")  
            
            # 性能监控：数据处理开始  
            start_time = time.time()  
            
            # 检查生产者线程状态  
            if not self.new_data_producer_thread or not self.new_data_producer_thread.is_alive():  
                logger.warning("Producer thread not running, attempting to restart")  
                self._update_performance_stats('threading', 'producer_restart')  
                if not self._restart_producer_thread():  
                    logger.error("Failed to restart producer thread")  
                    return -1  
            
            # 自动扩展块设备  
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
            
            with self._open_block_device('r+b') as f:  
                range_sink = RangeSinkState(rangeset, f, self.BLOCKSIZE)  
                
                blocks_to_write = rangeset.size  
                bytes_written = 0  
                
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
                        
                        bytes_written += written  
                        self.new_data_queue.task_done()  
                        
                    except queue.Empty:  
                        logger.error("Timeout waiting for new data")  
                        self._update_performance_stats('threading', 'queue_overflow')  
                        return -1  
            
            # 性能监控：数据处理完成  
            duration = time.time() - start_time  
            self._update_performance_stats('data', 'blocks_written', blocks=rangeset.size)  
            self._update_performance_stats('data', 'bytes_transferred', bytes=bytes_written)  
            self._update_performance_stats('io', 'write', bytes=bytes_written, duration=duration)  
            
            self.written += rangeset.size  
            return 0  
        
        except Exception as e:  
            logger.error(f"Failed to write new data: {e}")  
            self._update_performance_stats('error', error_type='new_command_failed')  
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
        """Apply diff patches with thread-safe patch data access"""    
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
                
                # Load source data using version 3 loader    
                try:    
                    _, src_data, _, _ = self._load_src_tgt_version3(tokens, pos - 4, onehash=False)    
                except Exception as e:    
                    logger.error(f"Failed to load source data: {e}")    
                    return -1    
                    
            else:    
                # Version 1/2 handling    
                if pos + 1 >= len(tokens):    
                    logger.error("Missing target/source ranges for version 1/2 diff")    
                    return -1    
                    
                tgt_rangeset = RangeSet(tokens[pos])    
                src_rangeset = RangeSet(tokens[pos + 1])    
                src_data = self._read_blocks(src_rangeset)    
                tgt_hash = None    
                
                if src_data is None:    
                    logger.error("Failed to read source blocks")    
                    return -1    
    
            # 使用锁保护补丁数据访问  
            with self.patch_data_lock:  
                # Extract patch data using memory mapping if available    
                if self.patch_data_mmap:    
                    patch_data = bytes(self.patch_data_mmap[offset:offset + length])    
                else:    
                    self.patch_data_fd.seek(offset)    
                    patch_data = self.patch_data_fd.read(length)    
    
            # Apply patch with proper error handling    
            result = self._apply_patch_internal(src_data, patch_data, cmd_name, tgt_hash)    
            if result is None:    
                return -1    
    
            # Write result using RangeSink with proper block alignment    
            if not self._write_blocks(tgt_rangeset, result):    
                logger.error("Failed to write patched blocks")    
                return -1    
    
            self.written += tgt_rangeset.size    
            return 0    
    
        except Exception as e:    
            logger.error(f"Failed to apply {cmd_name} patch: {e}")    
            return -1



    def _new_data_producer(self):    
        """Thread-safe producer thread with file access synchronization"""    
        logger.info("New data producer thread starting")    
        buffer_size = self.BLOCKSIZE * 10    
        
        try:    
            while self.new_data_producer_running.is_set():    
                try:    
                    # 使用锁保护文件访问  
                    with self.new_data_lock:  
                        # Check if file descriptor is still valid    
                        if not self.new_data_fd or self.new_data_fd.closed:    
                            logger.error("New data file descriptor is closed")    
                            break    
                        
                        # Check current position and file size    
                        current_pos = self.new_data_fd.tell()    
                        self.new_data_fd.seek(0, 2)  # Seek to end    
                        file_size = self.new_data_fd.tell()    
                        self.new_data_fd.seek(current_pos)  # Restore position    
                        
                        if current_pos >= file_size:    
                            logger.info("Reached end of new data file")    
                            self.new_data_queue.put(None)  # EOF signal    
                            break    
                        
                        # 读取数据时保持锁定  
                        data = self.new_data_fd.read(buffer_size)    
                        if not data:    
                            logger.info("No more data available, sending EOF signal")    
                            self.new_data_queue.put(None)  # EOF signal    
                            break    
                    
                    # 在锁外处理数据，避免长时间持有锁  
                    # Split into individual blocks    
                    for i in range(0, len(data), self.BLOCKSIZE):    
                        if not self.new_data_producer_running.is_set():    
                            break    
                        
                        block = data[i:i + self.BLOCKSIZE]    
                        if len(block) < self.BLOCKSIZE:    
                            block = block.ljust(self.BLOCKSIZE, b'\x00')    
                        
                        # Use timeout to avoid blocking indefinitely    
                        try:    
                            self.new_data_queue.put(block, timeout=5)    
                        except queue.Full:    
                            logger.warning("New data queue is full, producer may be blocked")    
                            continue    
                        
                        with self.new_data_condition:    
                            self.new_data_condition.notify_all()    
                    
                except (IOError, OSError) as e:    
                    logger.error(f"Producer I/O error: {e}")    
                    break    
                except Exception as e:    
                    logger.error(f"Producer unexpected error: {e}")    
                    break    
                    
        except Exception as e:    
            logger.error(f"Producer fatal error: {e}")    
        finally:    
            # Signal EOF and cleanup    
            try:    
                self.new_data_queue.put(None)    
            except:    
                pass    
            logger.info("New data producer thread terminating")

  
    def _apply_patch_internal(self, src_data: bytes, patch_data: bytes,     
                            cmd_name: str, expected_tgt_hash: str) -> Optional[bytes]:    
        """Apply patch using imported ApplyPatch functions with basic error handling"""    
        try:    
            # 确保 src_data 是 bytes 类型  
            if isinstance(src_data, bytearray):    
                src_data = bytes(src_data)    
            
            if patch_data.startswith(b'BSDIFF40'):    
                result = apply_bsdiff_patch(src_data, patch_data)    
            elif patch_data.startswith(b'IMGDIFF2'):    
                logger.info(f"Processing IMGDIFF2 patch: size={len(patch_data)}, src_size={len(src_data)}")  
                result = apply_imgdiff_patch_streaming(src_data, patch_data)    
            else:    
                logger.error(f"Unknown patch format: {patch_data[:8].hex()}")    
                return None    
    
            # 验证目标哈希  
            if expected_tgt_hash and expected_tgt_hash != "0" * 40:    
                actual_hash = hashlib.sha1(result).hexdigest()    
                if actual_hash != expected_tgt_hash:    
                    logger.error(f"Target hash mismatch: expected {expected_tgt_hash}, got {actual_hash}")    
                    return None    
    
            return result    
    
        except Exception as e:    
            logger.error(f"Failed to apply patch: {e}")    
            return None



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
        """Stash blocks for later use with LRU cache management"""    
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
                
            # Store in LRU cache (handles both memory and disk automatically)  
            self.stash_cache.put(stash_id, stash_data)  
            
            # Log cache statistics periodically  
            if logger.isEnabledFor(logging.DEBUG):  
                stats = self.stash_cache.get_memory_usage()  
                logger.debug(f"Stash cache stats: {stats}")  
                
            return 0    
                
        except Exception as e:    
            logger.error(f"Failed to stash blocks: {e}")    
            return -1

  
    def perform_command_free(self, tokens: List[str], pos: int) -> int:    
        """Free stashed data with LRU cache cleanup"""    
        if pos >= len(tokens):    
            logger.error("Missing stash id for free")    
            return -1    
                
        stash_id = tokens[pos]    
        
        # Remove from LRU cache (handles both memory and disk)  
        self.stash_cache.remove(stash_id)  
        logger.info(f"Freed stash {stash_id}")  
            
        return 0

  
    def _read_blocks(self, rangeset: RangeSet) -> Optional[bytes]:  
        """Read blocks with async optimization for large files"""  
        try:  
            # 对于大文件使用异步处理  
            if self.large_file_config.enable_streaming and rangeset.size > 256:  # >1MB  
                return asyncio.run(self._read_blocks_async(rangeset))  
            else:  
                return self._read_blocks_sync(rangeset)  
        except Exception as e:  
            logger.error(f"Failed to read blocks: {e}")  
            return None  
    
    async def _read_blocks_async(self, rangeset: RangeSet) -> bytes:  
        """Async version of block reading"""  
        async_processor = AsyncBlockProcessor(self.BLOCKSIZE)  
        
        with self._open_block_device('rb') as f:  
            return await async_processor.read_blocks_async(f, rangeset)  
  
    def _read_blocks_sync(self, rangeset: RangeSet) -> bytes:  
        """Synchronous version optimized for memory constraints"""  
        # For large ranges, use streaming approach  
        if rangeset.size > 64:  # >256KB  
            return self._read_blocks_streaming_sync(rangeset)  
        
        data = bytearray()  
        
        with self._open_block_device('rb') as f:  
            for start_block, end_block in rangeset:  
                f.seek(start_block * self.BLOCKSIZE)  
                
                # Process in very small chunks for memory constraint  
                remaining_blocks = end_block - start_block  
                while remaining_blocks > 0:  
                    chunk_blocks = min(remaining_blocks, 16)  # Only 64KB at a time  
                    chunk_size = chunk_blocks * self.BLOCKSIZE  
                    
                    chunk_data = f.read(chunk_size)  
                    if len(chunk_data) != chunk_size:  
                        logger.error(f"Short read: expected {chunk_size}, got {len(chunk_data)}")  
                        return None  
                    
                    data.extend(chunk_data)  
                    remaining_blocks -= chunk_blocks  
                    
                    # Aggressive memory management every 16 blocks  
                    if len(data) % (16 * self.BLOCKSIZE) == 0:  
                        gc.collect()  
        
        return bytes(data)

    def _read_blocks_streaming_sync(self, rangeset: RangeSet) -> bytes:  
        """Stream large block reads using temporary file to avoid memory pressure"""  
        temp_file = self.stash_base_dir / f"temp_read_{int(time.time())}.tmp"  
        
        try:  
            with temp_file.open('wb') as temp_fd, self._open_block_device('rb') as f:  
                for start_block, end_block in rangeset:  
                    f.seek(start_block * self.BLOCKSIZE)  
                    
                    remaining_blocks = end_block - start_block  
                    while remaining_blocks > 0:  
                        chunk_blocks = min(remaining_blocks, 8)  # 32KB chunks  
                        chunk_size = chunk_blocks * self.BLOCKSIZE  
                        
                        chunk_data = f.read(chunk_size)  
                        if len(chunk_data) != chunk_size:  
                            logger.error(f"Short read: expected {chunk_size}, got {len(chunk_data)}")  
                            return None  
                        
                        temp_fd.write(chunk_data)  
                        remaining_blocks -= chunk_blocks  
                        
                        # Force GC every 8 blocks  
                        gc.collect()  
            
            # Read back from temp file  
            return temp_file.read_bytes()  
            
        finally:  
            # Clean up temp file  
            if temp_file.exists():  
                temp_file.unlink()


    def _write_blocks(self, rangeset: RangeSet, data: bytes) -> bool:  
        """Write blocks with async optimization for large files"""  
        try:  
            # 对于大文件使用异步处理  
            if self.large_file_config.enable_streaming and len(data) > 1024 * 1024:  # >1MB  
                return asyncio.run(self._write_blocks_async(rangeset, data))  
            else:  
                return self._write_blocks_sync(rangeset, data)  
        except Exception as e:  
            logger.error(f"Failed to write blocks: {e}")  
            return False  
    
    async def _write_blocks_async(self, rangeset: RangeSet, data: bytes) -> bool:  
        """Async version of block writing"""  
        async_processor = AsyncBlockProcessor(self.BLOCKSIZE)  
        
        with self._open_block_device('r+b') as f:  
            success = await async_processor.write_blocks_async(f, rangeset, data)  
            
            if success:  
                # 异步同步  
                loop = asyncio.get_event_loop()  
                if self.io_settings.sync_method == 'fsync':  
                    await loop.run_in_executor(None, lambda: os.fsync(f.fileno()))  
                else:  
                    await loop.run_in_executor(None, f.flush)  
            
            return success  
    
    def _write_blocks_sync(self, rangeset: RangeSet, data: bytes) -> bool:  
        """Synchronous version for small files"""  
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
                    
                    # 定期进行内存管理  
                    if data_offset % (self.large_file_config.chunk_size_mb * 1024 * 1024) == 0:  
                        self._manage_memory()  
                
                # 同步  
                if self.io_settings.sync_method == 'fsync':  
                    os.fsync(f.fileno())  
                else:  
                    f.flush()  
            
            return True  
        except Exception as e:  
            logger.error(f"Failed to write blocks sync: {e}")  
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
  
    def _load_src_tgt_version2(self, tokens: List[str], pos: int) -> Tuple[RangeSet, bytes, int, bool]:    
        """Load source/target for version 2 commands - returns bytes consistently"""    
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
    
        # Handle stashes and ensure consistent return type  
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
    
        # Convert to bytes for consistent return type  
        return tgt_rangeset, bytes(src_data), src_block_count, overlap

    
    def _load_src_tgt_version3(self, tokens: List[str], pos: int, onehash: bool) -> Tuple[RangeSet, bytearray, int, bool]:  
        """  
        Parse and load source/target for version 3+ commands with LRU cache support.  
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
                # Try to recover from LRU cache  
                recovered_data = self.stash_cache.get(src_hash)  
                if recovered_data:  
                    # Ensure recovered data has the expected size  
                    if len(recovered_data) != src_block_count * self.BLOCKSIZE:  
                        raise ValueError(f"Recovered stash data size mismatch: expected {src_block_count * self.BLOCKSIZE}, got {len(recovered_data)}")  
                    src_data = bytearray(recovered_data)  
                    
                    if hashlib.sha1(src_data).hexdigest() != src_hash:  
                        raise ValueError("Partition has unexpected contents and stash recovery failed")  
                else:  
                    raise ValueError("Partition has unexpected contents")  
            else:  
                raise ValueError("Partition has unexpected contents")  
        else:  
            if overlap:  
                # Proactively store in LRU cache  
                self.stash_cache.put(src_hash, bytes(src_data))  
    
        return tgt_rangeset, src_data, src_block_count, overlap

    
    def _apply_stash_data(self, buffer: Union[bytearray, bytes], stash_spec: str):    
        """Apply stashed data to buffer at specified ranges with LRU cache"""    
        try:    
            stash_id, range_text = stash_spec.split(':', 1)    
            
            # Try to get from LRU cache  
            stash_data = self.stash_cache.get(stash_id)  
            
            if stash_data is None:  
                logger.warning(f"Stash ID {stash_id} not found in cache")    
                return buffer if isinstance(buffer, bytearray) else bytearray(buffer)    
    
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
        """Process transfer list commands with integrated performance monitoring"""  
        # 初始化性能监控  
        self._initialize_performance_monitoring()  
        
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
        
        failed_command_counts = {}  
        total_failed = 0  
        
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
                self._update_performance_stats('error', error_type='unknown_command')  
                if not self.continue_on_error:  
                    return -1  
                failed_command_counts[cmd] = failed_command_counts.get(cmd, 0) + 1  
                total_failed += 1  
                continue  
            
            logger.info(f"Executing: {cmd}")  
            
            # 性能监控：命令开始  
            self._update_performance_stats('command', 'start', command=cmd)  
            
            try:  
                result = commands[cmd](tokens, 1)  
                success = result == 0  
                
                # 性能监控：命令结束  
                self._update_performance_stats('command', 'end', command=cmd, success=success)  
                
                if result != 0:  
                    logger.error(f"Command {cmd} failed with code {result}")  
                    self._update_performance_stats('error', 'command_failed', error_type='command_failed')  
                    if self.continue_on_error:  
                        logger.warning(f"Continuing execution despite error in {cmd} command")  
                        failed_command_counts[cmd] = failed_command_counts.get(cmd, 0) + 1  
                        total_failed += 1  
                        continue  
                    else:  
                        return result  
                
                # 更新进度和保存检查点  
                self._update_and_save_progress(line_num, cmd)  
                
                # 定期内存管理和性能报告  
                if line_num % 20 == 0:  # 每20个命令  
                    self._manage_memory()  
                    if logger.isEnabledFor(logging.INFO):  
                        self._log_performance_summary()  
            
            except Exception as e:  
                logger.error(f"Exception executing command {cmd}: {e}")  
                self._update_performance_stats('command', 'end', command=cmd, success=False)  
                self._update_performance_stats('error', 'command_exception', error_type='command_exception')
                
                if self.continue_on_error:  
                    logger.warning(f"Continuing execution despite exception in {cmd} command")  
                    failed_command_counts[cmd] = failed_command_counts.get(cmd, 0) + 1  
                    total_failed += 1  
                    continue  
                else:  
                    return -1  
        
        # 最终性能报告  
        self._log_performance_summary()  
        report_file = self._save_performance_report()  
        if report_file:  
            logger.info(f"Detailed performance report available at: {report_file}")  
        
        # 如果有失败的命令，将详细信息存储到实例变量中  
        if total_failed > 0:  
            self.failed_command_details = failed_command_counts  
        
        return total_failed  



  
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


    def _manage_memory(self):  
        """Enhanced memory management for 2GB RAM constraint"""  
        current_time = time.time()  
        
        try:  
            process = psutil.Process()  
            memory_info = process.memory_info()  
            memory_mb = memory_info.rss // (1024 * 1024)  
            memory_percent = process.memory_percent()  
            
            # Update streaming state  
            self.streaming_state['memory_usage'] = memory_mb  
            
            # Much more aggressive thresholds for 2GB constraint  
            if memory_mb > 1024:  # Using more than 1GB  
                self._emergency_memory_cleanup()  
            elif memory_mb > 768:  # Using more than 768MB  
                self._aggressive_memory_cleanup()  
            elif memory_mb > 512:  # Using more than 512MB  
                self._gentle_memory_cleanup()  
            
            # Force GC every 10 seconds for memory constraint  
            if current_time - self._last_gc_time > 10:  
                collected = gc.collect()  
                if collected > 0:  
                    logger.debug(f"Forced GC: collected {collected} objects")  
                self._last_gc_time = current_time  
            
            # Monitor queue size and clear if too large  
            queue_size = self.new_data_queue.qsize()  
            if queue_size > 4:  # Very small queue for memory constraint  
                logger.warning(f"Queue size {queue_size} too large, clearing excess")  
                cleared = 0  
                while queue_size > 2 and not self.new_data_queue.empty():  
                    try:  
                        self.new_data_queue.get_nowait()  
                        cleared += 1  
                        queue_size -= 1  
                    except queue.Empty:  
                        break  
                if cleared > 0:  
                    logger.info(f"Cleared {cleared} queue items to manage memory")  
                    
        except Exception as e:  
            logger.warning(f"Memory management error: {e}")

    
    def _emergency_memory_cleanup(self):  
        """Emergency memory cleanup for critical memory pressure"""  
        logger.warning("Initiating emergency memory cleanup")  
        
        # 1. 清空大部分stash缓存  
        if hasattr(self, 'stash_cache'):  
            cache_items = list(self.stash_cache.cache.keys())  
            items_to_remove = len(cache_items) * 3 // 4  # 清理75%  
            for key in cache_items[:items_to_remove]:  
                self.stash_cache.remove(key)  
            logger.info(f"Emergency: cleared {items_to_remove} stash cache items")  
        
        # 2. 强制垃圾回收  
        for i in range(3):  
            collected = gc.collect()  
            if collected == 0:  
                break  
        
        # 3. 清理队列中的部分数据  
        cleared_items = 0  
        while not self.new_data_queue.empty() and cleared_items < 10:  
            try:  
                self.new_data_queue.get_nowait()  
                cleared_items += 1  
            except queue.Empty:  
                break  
        
        if cleared_items > 0:  
            logger.warning(f"Emergency: cleared {cleared_items} queue items")  
    
    def _aggressive_memory_cleanup(self):  
        """Aggressive memory cleanup for high memory pressure"""  
        logger.info("Initiating aggressive memory cleanup")  
        
        # 1. 清理一半的stash缓存  
        if hasattr(self, 'stash_cache'):  
            cache_items = list(self.stash_cache.cache.keys())  
            items_to_remove = len(cache_items) // 2  
            for key in cache_items[:items_to_remove]:  
                self.stash_cache.remove(key)  
            logger.info(f"Aggressive: cleared {items_to_remove} stash cache items")  
        
        # 2. 多次垃圾回收  
        total_collected = 0  
        for i in range(2):  
            collected = gc.collect()  
            total_collected += collected  
            if collected == 0:  
                break  
        
        if total_collected > 0:  
            logger.info(f"Aggressive: collected {total_collected} objects")  
    
    def _gentle_memory_cleanup(self):  
        """Gentle memory cleanup for moderate memory pressure"""  
        logger.debug("Initiating gentle memory cleanup")  
        
        # 1. 清理部分stash缓存（只清理最老的）  
        if hasattr(self, 'stash_cache'):  
            stats = self.stash_cache.get_memory_usage()  
            if stats['memory_mb'] > 50:  # 如果stash缓存超过50MB  
                cache_items = list(self.stash_cache.cache.keys())  
                items_to_remove = min(5, len(cache_items) // 4)  # 清理最多5个或25%  
                for key in cache_items[:items_to_remove]:  
                    self.stash_cache.remove(key)  
                logger.debug(f"Gentle: cleared {items_to_remove} stash cache items")  
        
        # 2. 单次垃圾回收  
        collected = gc.collect()  
        if collected > 0:  
            logger.debug(f"Gentle: collected {collected} objects")  
    
    def _log_memory_stats(self, memory_mb: int, memory_percent: float):  
        """Log detailed memory statistics for debugging"""  
        try:  
            # 系统内存信息  
            system_memory = psutil.virtual_memory()  
            
            # Stash缓存信息  
            stash_stats = {}  
            if hasattr(self, 'stash_cache'):  
                stash_stats = self.stash_cache.get_memory_usage()  
            
            # 队列信息  
            queue_size = self.new_data_queue.qsize()  
            queue_max = self.new_data_queue.maxsize  
            
            # 流式处理状态  
            streaming_info = self.streaming_state.copy()  
            
            logger.debug(f"Memory Stats - Process: {memory_mb}MB ({memory_percent:.1f}%), "  
                        f"System: {system_memory.percent:.1f}% used, "  
                        f"Stash: {stash_stats.get('memory_mb', 0):.1f}MB, "  
                        f"Queue: {queue_size}/{queue_max}, "  
                        f"Streaming: {streaming_info}")  
                        
        except Exception as e:  
            logger.debug(f"Failed to log memory stats: {e}")  
    
    def _adjust_processing_strategy(self):  
        """Dynamically adjust processing strategy based on memory usage"""  
        try:  
            memory_percent = psutil.Process().memory_percent()  
            
            # 根据内存使用情况调整队列大小  
            if memory_percent > 80:  
                # 高内存压力：减小队列  
                new_maxsize = max(5, self.new_data_queue.maxsize // 2)  
            elif memory_percent < 40:  
                # 低内存压力：可以增大队列  
                new_maxsize = min(50, self.new_data_queue.maxsize * 2)  
            else:  
                return  # 内存使用正常，不调整  
            
            # 注意：Python的Queue不支持动态调整maxsize，这里只是记录建议值  
            logger.debug(f"Suggested queue size adjustment: {self.new_data_queue.maxsize} -> {new_maxsize}")  
            
            # 调整垃圾回收频率  
            if memory_percent > 70:  
                self.large_file_config.gc_frequency = max(5, self.large_file_config.gc_frequency - 2)  
            elif memory_percent < 50:  
                self.large_file_config.gc_frequency = min(20, self.large_file_config.gc_frequency + 2)  
                
        except Exception as e:  
            logger.debug(f"Failed to adjust processing strategy: {e}")


    def _enhanced_resume_check(self) -> int:  
        """Enhanced resume check with integrity verification"""  
        start_line = 4 if self.version >= 2 else 2  
        
        # 加载进度检查点  
        checkpoint_line = self._load_progress_checkpoint()  
        if checkpoint_line > start_line:  
            # 验证检查点的完整性  
            if self._verify_checkpoint_integrity(checkpoint_line):  
                logger.info(f"Resuming from verified checkpoint at line {checkpoint_line}")  
                return checkpoint_line  
            else:  
                logger.warning("Checkpoint integrity verification failed, performing full verification")  
        
        # 执行完整的哈希验证  
        return self._comprehensive_hash_verification()  
    
    def _verify_checkpoint_integrity(self, checkpoint_line: int) -> bool:  
        """Verify the integrity of a checkpoint by checking recent operations"""  
        try:  
            # 检查最近几个命令的完整性  
            verification_range = max(0, checkpoint_line - 5)  
            
            for line_num in range(verification_range, checkpoint_line):  
                if line_num >= len(self.transfer_lines):  
                    continue  
                    
                line = self.transfer_lines[line_num].strip()  
                if not line:  
                    continue  
                    
                tokens = line.split()  
                if not tokens:  
                    continue  
                    
                cmd = tokens[0]  
                
                # 验证关键命令的完整性  
                if cmd in ['bsdiff', 'imgdiff', 'new', 'move']:  
                    if not self._verify_command_integrity(tokens, cmd):  
                        logger.warning(f"Command integrity check failed at line {line_num}: {cmd}")  
                        return False  
            
            return True  
            
        except Exception as e:  
            logger.error(f"Checkpoint integrity verification failed: {e}")  
            return False  
    
    def _verify_command_integrity(self, tokens: List[str], cmd_name: str) -> bool:  
        """Verify the integrity of a specific command"""  
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
                        # 检查块是否已被正确写入  
                        return not all(b == 0 for b in actual_data)  
            elif cmd_name == 'move':  
                # 对于move命令，检查目标范围是否有数据  
                if len(tokens) >= 2:  
                    tgt_range = tokens[1] if self.version == 1 else tokens[3]  
                    tgt_rangeset = RangeSet(tgt_range)  
                    actual_data = self._read_blocks(tgt_rangeset)  
                    return actual_data is not None  
            
            return True  
            
        except Exception as e:  
            logger.debug(f"Command integrity check error: {e}")  
            return False  
    
    def _comprehensive_hash_verification(self) -> int:  
        """Comprehensive hash verification for reliable resume point detection"""  
        start_line = 4 if self.version >= 2 else 2  
        last_verified_line = start_line  
        
        logger.info("Performing comprehensive hash verification for resume point detection")  
        
        for line_num in range(start_line, len(self.transfer_lines)):  
            line = self.transfer_lines[line_num].strip()  
            if not line:  
                continue  
                
            tokens = line.split()  
            if not tokens:  
                continue  
                
            cmd = tokens[0]  
            
            # 只检查会修改数据的命令  
            if cmd in ['bsdiff', 'imgdiff', 'new', 'move']:  
                if self._verify_command_integrity(tokens, cmd):  
                    last_verified_line = line_num + 1  
                else:  
                    logger.info(f"Found incomplete operation at line {line_num}, resuming from line {last_verified_line}")  
                    return last_verified_line  
        
        logger.info("All operations verified as complete")  
        return len(self.transfer_lines)  # 所有操作都已完成

    def _verify_data_integrity(self, data: bytes, expected_hash: str = None, operation: str = "unknown") -> bool:  
        """Verify data integrity with optional hash checking"""  
        try:  
            # 基本数据检查  
            if not data:  
                logger.error(f"Data integrity check failed: empty data for {operation}")  
                return False  
            
            # 检查数据是否全为零（可能表示损坏）  
            if len(data) > self.BLOCKSIZE and all(b == 0 for b in data):  
                logger.warning(f"Data integrity warning: all-zero data detected for {operation}")  
            
            # 哈希验证  
            if expected_hash and expected_hash != "0" * 40:  
                actual_hash = hashlib.sha1(data).hexdigest()  
                if actual_hash != expected_hash:  
                    logger.error(f"Data integrity check failed: hash mismatch for {operation}")  
                    logger.error(f"Expected: {expected_hash}, Got: {actual_hash}")  
                    return False  
            
            # 检查数据模式（检测可能的损坏模式）  
            if self._detect_corruption_patterns(data):  
                logger.warning(f"Potential data corruption patterns detected for {operation}")  
                return False  
            
            return True  
            
        except Exception as e:  
            logger.error(f"Data integrity verification failed for {operation}: {e}")  
            return False  
    
    def _detect_corruption_patterns(self, data: bytes) -> bool:  
        """Detect common data corruption patterns"""  
        try:  
            # 检查重复模式（可能表示损坏）  
            if len(data) >= 1024:  
                chunk_size = 256  
                chunks = [data[i:i+chunk_size] for i in range(0, min(len(data), 1024), chunk_size)]  
                
                # 如果所有块都相同，可能是损坏  
                if len(set(chunks)) == 1 and chunks[0] != b'\x00' * chunk_size:  
                    logger.debug("Detected repeated pattern in data")  
                    return True  
            
            # 检查异常的字节分布  
            if len(data) >= 1024:  
                byte_counts = {}  
                sample_size = min(1024, len(data))  
                for byte in data[:sample_size]:  
                    byte_counts[byte] = byte_counts.get(byte, 0) + 1  
                
                # 如果某个字节值占比超过90%，可能是损坏  
                max_count = max(byte_counts.values())  
                if max_count > sample_size * 0.9:  
                    logger.debug("Detected unusual byte distribution")  
                    return True  
            
            return False  
            
        except Exception as e:  
            logger.debug(f"Corruption pattern detection failed: {e}")  
            return False  
    
    def _repair_corrupted_data(self, rangeset: RangeSet, expected_hash: str = None) -> Optional[bytes]:  
        """Attempt to repair corrupted data using various strategies"""  
        logger.info(f"Attempting to repair corrupted data for {rangeset.size} blocks")  
        
        # 策略1：从stash缓存恢复  
        if expected_hash:  
            cached_data = self.stash_cache.get(expected_hash)  
            if cached_data and self._verify_data_integrity(cached_data, expected_hash, "cache_recovery"):  
                logger.info("Successfully recovered data from stash cache")  
                return cached_data  
        
        # 策略2：重新读取数据（可能是临时I/O错误）  
        try:  
            logger.info("Attempting to re-read data")  
            recovered_data = self._read_blocks_with_retry(rangeset, max_retries=3)  
            if recovered_data and self._verify_data_integrity(recovered_data, expected_hash, "re_read"):  
                logger.info("Successfully recovered data by re-reading")  
                return recovered_data  
        except Exception as e:  
            logger.warning(f"Re-read attempt failed: {e}")  
        
        # 策略3：从备份位置恢复（如果有的话）  
        backup_data = self._try_backup_recovery(rangeset, expected_hash)  
        if backup_data:  
            return backup_data  
        
        logger.error("All data recovery strategies failed")  
        return None  
    
    def _read_blocks_with_retry(self, rangeset: RangeSet, max_retries: int = 3) -> Optional[bytes]:  
        """Read blocks with retry logic for handling transient errors"""  
        for attempt in range(max_retries):  
            try:  
                # 在每次重试之间稍作等待  
                if attempt > 0:  
                    time.sleep(0.1 * attempt)  
                
                data = self._read_blocks_sync(rangeset)  
                if data:  
                    return data  
                    
            except Exception as e:  
                logger.warning(f"Read attempt {attempt + 1} failed: {e}")  
                if attempt == max_retries - 1:  
                    raise  
        
        return None  
    
    def _try_backup_recovery(self, rangeset: RangeSet, expected_hash: str = None) -> Optional[bytes]:  
        """Try to recover data from backup locations"""  
        try:  
            # 检查是否有备份文件  
            backup_file = self.stash_base_dir / f"backup_{expected_hash}" if expected_hash else None  
            if backup_file and backup_file.exists():  
                backup_data = backup_file.read_bytes()  
                if self._verify_data_integrity(backup_data, expected_hash, "backup_recovery"):  
                    logger.info("Successfully recovered data from backup file")  
                    return backup_data  
            
            return None  
            
        except Exception as e:  
            logger.warning(f"Backup recovery failed: {e}")  
            return None

    def _initialize_performance_monitoring(self):  
        """Initialize comprehensive performance monitoring and diagnostics"""  
        self.performance_stats = {  
            'session': {  
                'start_time': time.time(),  
                'session_id': hashlib.md5(str(time.time()).encode()).hexdigest()[:8],  
                'total_runtime': 0,  
                'completion_percentage': 0  
            },  
            'commands': {  
                'executed': 0,  
                'failed': 0,  
                'skipped': 0,  
                'timings': {},  
                'error_details': {}  
            },  
            'data_processing': {  
                'blocks_read': 0,  
                'blocks_written': 0,  
                'bytes_transferred': 0,  
                'compression_ratio': 0,  
                'patch_efficiency': {}  
            },  
            'memory': {  
                'peak_usage_mb': 0,  
                'current_usage_mb': 0,  
                'gc_collections': 0,  
                'cache_efficiency': {  
                    'hits': 0,  
                    'misses': 0,  
                    'evictions': 0  
                }  
            },  
            'io_performance': {  
                'read_operations': 0,  
                'write_operations': 0,  
                'read_throughput_mbps': 0,  
                'write_throughput_mbps': 0,  
                'seek_operations': 0,  
                'sync_operations': 0  
            },  
            'threading': {  
                'producer_restarts': 0,  
                'queue_overflows': 0,  
                'lock_contentions': 0,  
                'thread_efficiency': 0  
            },  
            'errors_and_recovery': {  
                'recoverable_errors': 0,  
                'fatal_errors': 0,  
                'recovery_attempts': 0,  
                'recovery_success_rate': 0  
            }  
        }  
        
        # 初始化性能监控定时器  
        self._last_stats_update = time.time()  
        self._stats_update_interval = 5.0  # 每5秒更新一次统计  
    
    def _update_performance_stats(self, category: str, operation: str, **kwargs):  
        """Update performance statistics with detailed categorization"""  
        try:  
            current_time = time.time()  
            
            if category == 'command':  
                self._update_command_stats(operation, current_time, **kwargs)  
            elif category == 'data':  
                self._update_data_stats(operation, **kwargs)  
            elif category == 'memory':  
                self._update_memory_stats(operation, **kwargs)  
            elif category == 'io':  
                self._update_io_stats(operation, current_time, **kwargs)  
            elif category == 'threading':  
                self._update_threading_stats(operation, **kwargs)  
            elif category == 'error':  
                self._update_error_stats(operation, **kwargs)  
            
            # 定期计算综合指标  
            if current_time - self._last_stats_update > self._stats_update_interval:  
                self._calculate_derived_metrics()  
                self._last_stats_update = current_time  
                
        except Exception as e:  
            logger.debug(f"Performance stats update failed: {e}")  
    
    def _update_command_stats(self, operation: str, current_time: float, **kwargs):  
        """Update command-specific statistics"""  
        stats = self.performance_stats['commands']  
        
        if operation == 'start':  
            cmd = kwargs.get('command')  
            if cmd not in stats['timings']:  
                stats['timings'][cmd] = {  
                    'count': 0,  
                    'total_time': 0,  
                    'avg_time': 0,  
                    'min_time': float('inf'),  
                    'max_time': 0,  
                    'current_start': current_time  
                }  
            else:  
                stats['timings'][cmd]['current_start'] = current_time  
            stats['executed'] += 1  
            
        elif operation == 'end':  
            cmd = kwargs.get('command')  
            success = kwargs.get('success', True)  
            
            if cmd in stats['timings'] and 'current_start' in stats['timings'][cmd]:  
                duration = current_time - stats['timings'][cmd]['current_start']  
                cmd_stats = stats['timings'][cmd]  
                
                cmd_stats['count'] += 1  
                cmd_stats['total_time'] += duration  
                cmd_stats['avg_time'] = cmd_stats['total_time'] / cmd_stats['count']  
                cmd_stats['min_time'] = min(cmd_stats['min_time'], duration)  
                cmd_stats['max_time'] = max(cmd_stats['max_time'], duration)  
                
                if not success:  
                    stats['failed'] += 1  
                    if cmd not in stats['error_details']:  
                        stats['error_details'][cmd] = 0  
                    stats['error_details'][cmd] += 1  
    
    def _update_data_stats(self, operation: str, **kwargs):  
        """Update data processing statistics"""  
        stats = self.performance_stats['data_processing']  
        
        if operation == 'blocks_read':  
            blocks = kwargs.get('blocks', 0)  
            stats['blocks_read'] += blocks  
            
        elif operation == 'blocks_written':  
            blocks = kwargs.get('blocks', 0)  
            stats['blocks_written'] += blocks  
            
        elif operation == 'bytes_transferred':  
            bytes_count = kwargs.get('bytes', 0)  
            stats['bytes_transferred'] += bytes_count  
            
        elif operation == 'patch_applied':  
            patch_type = kwargs.get('patch_type', 'unknown')  
            src_size = kwargs.get('src_size', 0)  
            patch_size = kwargs.get('patch_size', 0)  
            
            if patch_type not in stats['patch_efficiency']:  
                stats['patch_efficiency'][patch_type] = {  
                    'count': 0,  
                    'total_src_size': 0,  
                    'total_patch_size': 0,  
                    'avg_compression': 0  
                }  
            
            patch_stats = stats['patch_efficiency'][patch_type]  
            patch_stats['count'] += 1  
            patch_stats['total_src_size'] += src_size  
            patch_stats['total_patch_size'] += patch_size  
            
            if patch_stats['total_src_size'] > 0:  
                patch_stats['avg_compression'] = patch_stats['total_patch_size'] / patch_stats['total_src_size']  
    
    def _update_memory_stats(self, operation: str, **kwargs):  
        """Update memory usage statistics"""  
        stats = self.performance_stats['memory']  
        
        if operation == 'usage_update':  
            memory_mb = kwargs.get('memory_mb', 0)  
            stats['current_usage_mb'] = memory_mb  
            stats['peak_usage_mb'] = max(stats['peak_usage_mb'], memory_mb)  
            
        elif operation == 'gc_collection':  
            stats['gc_collections'] += 1  
            
        elif operation == 'cache_hit':  
            stats['cache_efficiency']['hits'] += 1  
            
        elif operation == 'cache_miss':  
            stats['cache_efficiency']['misses'] += 1  
            
        elif operation == 'cache_eviction':  
            stats['cache_efficiency']['evictions'] += 1  
    
    def _update_io_stats(self, operation: str, current_time: float, **kwargs):  
        """Update I/O performance statistics"""  
        stats = self.performance_stats['io_performance']  
        
        if operation == 'read':  
            stats['read_operations'] += 1  
            bytes_read = kwargs.get('bytes', 0)  
            duration = kwargs.get('duration', 0)  
            
            if duration > 0:  
                throughput_mbps = (bytes_read / (1024 * 1024)) / duration  
                # 使用移动平均计算吞吐量  
                if stats['read_throughput_mbps'] == 0:  
                    stats['read_throughput_mbps'] = throughput_mbps  
                else:  
                    stats['read_throughput_mbps'] = (stats['read_throughput_mbps'] * 0.8 + throughput_mbps * 0.2)  
                    
        elif operation == 'write':  
            stats['write_operations'] += 1  
            bytes_written = kwargs.get('bytes', 0)  
            duration = kwargs.get('duration', 0)  
            
            if duration > 0:  
                throughput_mbps = (bytes_written / (1024 * 1024)) / duration  
                if stats['write_throughput_mbps'] == 0:  
                    stats['write_throughput_mbps'] = throughput_mbps  
                else:  
                    stats['write_throughput_mbps'] = (stats['write_throughput_mbps'] * 0.8 + throughput_mbps * 0.2)  
                    
        elif operation == 'seek':  
            stats['seek_operations'] += 1  
            
        elif operation == 'sync':  
            stats['sync_operations'] += 1  
    
    def _update_threading_stats(self, operation: str, **kwargs):  
        """Update threading performance statistics"""  
        stats = self.performance_stats['threading']  
        
        if operation == 'producer_restart':  
            stats['producer_restarts'] += 1  
            
        elif operation == 'queue_overflow':  
            stats['queue_overflows'] += 1  
            
        elif operation == 'lock_contention':  
            stats['lock_contentions'] += 1  
            
        elif operation == 'efficiency_update':  
            efficiency = kwargs.get('efficiency', 0)  
            stats['thread_efficiency'] = efficiency  
    
    def _update_error_stats(self, operation: str, **kwargs):  
        """Update error and recovery statistics"""  
        stats = self.performance_stats['errors_and_recovery']  
        
        if operation == 'recoverable_error':  
            stats['recoverable_errors'] += 1  
            
        elif operation == 'fatal_error':  
            stats['fatal_errors'] += 1  
            
        elif operation == 'recovery_attempt':  
            stats['recovery_attempts'] += 1  
            
        elif operation == 'recovery_success':  
            if stats['recovery_attempts'] > 0:  
                success_count = kwargs.get('success_count', 1)  
                stats['recovery_success_rate'] = success_count / stats['recovery_attempts']  
    
    def _calculate_derived_metrics(self):  
        """Calculate derived performance metrics"""  
        try:  
            current_time = time.time()  
            session_stats = self.performance_stats['session']  
            
            # 更新会话统计  
            session_stats['total_runtime'] = current_time - session_stats['start_time']  
            
            # 计算完成百分比  
            if self.total_blocks > 0:  
                session_stats['completion_percentage'] = (self.written / self.total_blocks) * 100  
            
            # 计算缓存效率  
            cache_stats = self.performance_stats['memory']['cache_efficiency']  
            total_cache_ops = cache_stats['hits'] + cache_stats['misses']  
            if total_cache_ops > 0:  
                cache_stats['hit_rate'] = cache_stats['hits'] / total_cache_ops  
            
            # 计算线程效率  
            threading_stats = self.performance_stats['threading']  
            if threading_stats['producer_restarts'] > 0:  
                restart_rate = threading_stats['producer_restarts'] / session_stats['total_runtime']  
                threading_stats['thread_efficiency'] = max(0, 1 - restart_rate)  
            
        except Exception as e:  
            logger.debug(f"Derived metrics calculation failed: {e}")  
    
    def _generate_performance_report(self) -> dict:  
        """Generate comprehensive performance report with insights"""  
        try:  
            self._calculate_derived_metrics()  
            
            report = {  
                'summary': self._generate_summary_metrics(),  
                'detailed_stats': self.performance_stats.copy(),  
                'performance_insights': self._generate_performance_insights(),  
                'recommendations': self._generate_recommendations(),  
                'timestamp': time.time()  
            }  
            
            return report  
            
        except Exception as e:  
            logger.error(f"Performance report generation failed: {e}")  
            return {'error': str(e)}  
    
    def _generate_summary_metrics(self) -> dict:  
        """Generate high-level summary metrics"""  
        session = self.performance_stats['session']  
        commands = self.performance_stats['commands']  
        data = self.performance_stats['data_processing']  
        memory = self.performance_stats['memory']  
        io = self.performance_stats['io_performance']  
        
        return {  
            'runtime_minutes': session['total_runtime'] / 60,  
            'completion_percentage': session['completion_percentage'],  
            'commands_per_minute': commands['executed'] / max(session['total_runtime'] / 60, 1),  
            'error_rate': commands['failed'] / max(commands['executed'], 1),  
            'data_throughput_mbps': data['bytes_transferred'] / (1024 * 1024) / max(session['total_runtime'], 1),  
            'memory_efficiency': memory['peak_usage_mb'] / max(memory['current_usage_mb'], 1),  
            'io_efficiency': (io['read_throughput_mbps'] + io['write_throughput_mbps']) / 2,  
            'cache_hit_rate': memory['cache_efficiency'].get('hit_rate', 0)  
        }  
    
    def _generate_performance_insights(self) -> List[str]:  
        """Generate performance insights and observations"""  
        insights = []  
        summary = self._generate_summary_metrics()  
        
        # 性能洞察  
        if summary['error_rate'] > 0.1:  
            insights.append(f"High error rate detected: {summary['error_rate']:.1%}")  
        
        if summary['cache_hit_rate'] < 0.8:  
            insights.append(f"Low cache efficiency: {summary['cache_hit_rate']:.1%} hit rate")  
        
        if summary['memory_efficiency'] > 2.0:  
            insights.append("Memory usage is highly variable, consider optimization")  
        
        if summary['io_efficiency'] < 10:  
            insights.append(f"Low I/O throughput: {summary['io_efficiency']:.1f} MB/s average")  
        
        # 线程性能洞察  
        threading_stats = self.performance_stats['threading']  
        if threading_stats['producer_restarts'] > 5:  
            insights.append(f"Frequent producer thread restarts: {threading_stats['producer_restarts']}")  
        
        return insights  

    def _generate_recommendations(self) -> List[str]:  
        """Generate performance optimization recommendations"""  
        recommendations = []  
        summary = self._generate_summary_metrics()  
        
        if summary['cache_hit_rate'] < 0.7:  
            recommendations.append("Consider increasing cache size or adjusting eviction policy")  
        
        if summary['error_rate'] > 0.05:  
            recommendations.append("Review error handling and add more robust retry mechanisms")  
        
        if summary['io_efficiency'] < 20:  
            recommendations.append("Consider using larger I/O buffer sizes or async I/O")  
        
        if self.performance_stats['memory']['peak_usage_mb'] > 1024:  
            recommendations.append("Consider implementing more aggressive memory management")  
        
        return recommendations  

    def __exit__(self, exc_type, exc_val, exc_tb):  
        """Enhanced cleanup with proper resource management and cache cleanup"""  
        logger.info("Cleaning up BlockImageUpdate resources")  
        
        # Stop producer thread  
        if self.new_data_producer_thread and self.new_data_producer_thread.is_alive():  
            self.new_data_producer_running.clear()  
            with self.new_data_condition:  
                self.new_data_condition.notify_all()  
            self.new_data_producer_thread.join(timeout=5)  
        
        # Close file descriptors and memory maps with specific error handling  
        if self.patch_data_mmap:  
            try:  
                self.patch_data_mmap.close()  
                self.patch_data_mmap = None  
            except Exception as e:  
                logger.warning(f"Failed to close patch data mmap: {e}")  
        
        if self.new_data_fd:  
            try:  
                self.new_data_fd.close()  
                self.new_data_fd = None  
            except Exception as e:  
                logger.warning(f"Failed to close new data file: {e}")  
        
        if self.patch_data_fd:  
            try:  
                self.patch_data_fd.close()  
                self.patch_data_fd = None  
            except Exception as e:  
                logger.warning(f"Failed to close patch data file: {e}")  
        
        # Clean up progress checkpoint on normal completion  
        if exc_type is None:  
            self._cleanup_progress_checkpoint()  
        
        # Cleanup stash (now includes LRU cache cleanup)  
        self._cleanup_stash()  
        
        # Clear queue  
        try:  
            while not self.new_data_queue.empty():  
                try:  
                    self.new_data_queue.get_nowait()  
                    self.new_data_queue.task_done()  
                except queue.Empty:  
                    break  
        except Exception as e:  
            logger.warning(f"Failed to clear queue: {e}")
  

    def _log_performance_summary(self):  
        """Log a concise performance summary with key metrics"""  
        try:  
            summary = self._generate_summary_metrics()  
            session = self.performance_stats['session']  
            
            # 基本会话信息  
            logger.info(f"=== Performance Summary (Session: {session['session_id']}) ===")  
            logger.info(f"Runtime: {summary['runtime_minutes']:.1f} minutes")  
            logger.info(f"Progress: {summary['completion_percentage']:.1f}% complete")  
            
            # 命令执行统计  
            commands = self.performance_stats['commands']  
            logger.info(f"Commands: {commands['executed']} executed, {commands['failed']} failed")  
            logger.info(f"Command rate: {summary['commands_per_minute']:.1f} commands/min")  
            
            # 数据处理统计  
            data = self.performance_stats['data_processing']  
            logger.info(f"Data: {data['blocks_written']} blocks written, {summary['data_throughput_mbps']:.1f} MB/s")  
            
            # 内存和缓存统计  
            memory = self.performance_stats['memory']  
            logger.info(f"Memory: {memory['current_usage_mb']:.0f}MB current, {memory['peak_usage_mb']:.0f}MB peak")  
            logger.info(f"Cache: {summary['cache_hit_rate']:.1%} hit rate, {memory['cache_efficiency']['evictions']} evictions")  
            
            # I/O性能统计  
            io = self.performance_stats['io_performance']  
            logger.info(f"I/O: {io['read_operations']} reads, {io['write_operations']} writes")  
            logger.info(f"Throughput: R={summary.get('read_throughput_mbps', 0):.1f} W={summary.get('write_throughput_mbps', 0):.1f} MB/s")  
            
            # 线程和错误统计  
            threading = self.performance_stats['threading']  
            errors = self.performance_stats['errors_and_recovery']  
            if threading['producer_restarts'] > 0 or errors['recoverable_errors'] > 0:  
                logger.info(f"Issues: {threading['producer_restarts']} thread restarts, {errors['recoverable_errors']} recoverable errors")  
            
            # 性能洞察  
            insights = self._generate_performance_insights()  
            if insights:  
                logger.info("Performance insights:")  
                for insight in insights[:3]:  # 只显示前3个最重要的洞察  
                    logger.info(f"  • {insight}")  
            
            logger.info("=" * 50)  
            
        except Exception as e:  
            logger.warning(f"Failed to log performance summary: {e}")  
    
    def _save_performance_report(self, filename: str = None):  
        """Save detailed performance report to file"""  
        try:  
            if not filename:  
                timestamp = time.strftime("%Y%m%d_%H%M%S")  
                filename = f"performance_report_{timestamp}.json"  
            
            report = self._generate_performance_report()  
            
            report_file = self.stash_base_dir / filename  
            with open(report_file, 'w', encoding='utf-8') as f:  
                json.dump(report, f, indent=2, ensure_ascii=False)  
            
            logger.info(f"Performance report saved to: {report_file}")  
            return str(report_file)  
            
        except Exception as e:  
            logger.error(f"Failed to save performance report: {e}")  
            return None  
    
    def _export_performance_metrics(self) -> dict:  
        """Export performance metrics for external monitoring systems"""  
        try:  
            summary = self._generate_summary_metrics()  
            
            # 导出关键指标，格式适合监控系统  
            metrics = {  
                'biu_runtime_seconds': self.performance_stats['session']['total_runtime'],  
                'biu_completion_percentage': summary['completion_percentage'],  
                'biu_commands_executed_total': self.performance_stats['commands']['executed'],  
                'biu_commands_failed_total': self.performance_stats['commands']['failed'],  
                'biu_error_rate': summary['error_rate'],  
                'biu_blocks_written_total': self.performance_stats['data_processing']['blocks_written'],  
                'biu_bytes_transferred_total': self.performance_stats['data_processing']['bytes_transferred'],  
                'biu_memory_usage_mb': self.performance_stats['memory']['current_usage_mb'],  
                'biu_memory_peak_mb': self.performance_stats['memory']['peak_usage_mb'],  
                'biu_cache_hit_rate': summary['cache_hit_rate'],  
                'biu_io_read_ops_total': self.performance_stats['io_performance']['read_operations'],  
                'biu_io_write_ops_total': self.performance_stats['io_performance']['write_operations'],  
                'biu_thread_restarts_total': self.performance_stats['threading']['producer_restarts'],  
                'biu_recoverable_errors_total': self.performance_stats['errors_and_recovery']['recoverable_errors']  
            }  
            
            return metrics  
            
        except Exception as e:  
            logger.error(f"Failed to export performance metrics: {e}")  
            return {}


  
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
                # 生成详细的失败命令报告  
                if hasattr(biu, 'failed_command_details') and biu.failed_command_details:  
                    failure_details = []  
                    for cmd, count in sorted(biu.failed_command_details.items()):  
                        failure_details.append(f"{cmd}*{count}")  
                    failure_summary = " ".join(failure_details)  
                    print(f"Done with {result} failed commands ({failure_summary}) (continued execution)")  
                else:  
                    print(f"Done with {result} failed commands (continued execution)")  
            else:  
                print(f"Done with error code: {result}")  
  
            return result  
    except Exception as e:  
        print(f"Fatal error: {e}")  
        return -1

class ErrorLogHandler:  
    log_file = 'errors.txt'  # Add this class attribute  
      
    def __init__(self, log_file='errors.txt'):  
        self.log_file = log_file  # Keep instance attribute too  
        self._setup_logger()  
  
    def _setup_logger(self):  
        """Configure logger to retain current run logs and capture all warning+ level info"""  
        self.logger = logging.getLogger('error_logger')  
        self.logger.setLevel(logging.WARNING)  # Record WARNING and above levels  
          
        # File handler (auto-overwrite each run)  
        file_handler = logging.FileHandler(self.log_file, mode='w', encoding='utf-8')  
        file_handler.setFormatter(logging.Formatter(  
            '[%(asctime)s] %(levelname)s: %(message)s\n'  
            'File: %(filename)s:%(lineno)d\n\n'  
        ))  
  
        # Console handler (optional)  
        console_handler = logging.StreamHandler()  
        console_handler.setLevel(logging.INFO)  # Console shows INFO and above only  
          
        self.logger.addHandler(file_handler)  
        self.logger.addHandler(console_handler)  
  
    def capture_errors(self, func, *args, **kwargs):  
        """Decorator pattern to capture errors during function execution"""  
        try:  
            with redirect_stderr(self.logger.handlers[0].stream):  
                return func(*args, **kwargs)  
        except Exception as e:  
            self.logger.error(f"Critical error occurred: {str(e)}", exc_info=True)  
            raise

def main():  
    """Enhanced main function with logging and detailed error display"""  
    # Initialize logging system (auto-clear old logs)  
    error_handler = ErrorLogHandler()  
  
    # Parameter parsing  
    if len(sys.argv) < 5:  
        print(f"Usage: {sys.argv[0]} <system.img> <system.transfer.list> "  
              "<system.new.dat> <system.patch.dat> [--continue-on-error]")  
        return 1  
  
    continue_on_error = '--continue-on-error' in sys.argv  
  
    # Main processing flow  
    try:  
        with BlockImageUpdate(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],  
                             continue_on_error=continue_on_error) as biu:  
            result = biu.process_transfer_list()  
  
            if result == 0:  
                print("Done")  
            else:  
                error_message = f"Done with error code: {result}"  
                if continue_on_error and hasattr(biu, 'failed_command_details'):  
                    failure_details = ", ".join(  
                        f"{cmd}:{count}"   
                        for cmd, count in sorted(biu.failed_command_details.items())  
                    )  
                    error_message += f" (Failed commands: {failure_details})"  
                      
                # Print the detailed error message to console  
                print(f"Error: {error_message}")  
                error_handler.logger.error(error_message)  
                return result  
  
    except Exception as e:  
        # Print the specific exception details to console  
        print(f"Fatal error occurred: {str(e)}")  
        print(f"Error type: {type(e).__name__}")  
          
        # If it's a specific BIU-related error, provide more context  
        if hasattr(e, '__traceback__'):  
            import traceback  
            print("Error traceback:")  
            traceback.print_exc()  
          
        # Also log to file for detailed analysis  
        error_handler.logger.error(f"Fatal error: {str(e)}", exc_info=True)  
        print(f"\nDetailed error log saved to: {error_handler.log_file}")  
        return -1  
  
    return 0


  
if __name__ == "__main__":  
    sys.exit(main())
    