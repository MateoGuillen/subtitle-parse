import asyncio
import os
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import pdfplumber
import pyarrow as pa
import pyarrow.parquet as pq
import re
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import platform
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import io
from functools import partial
import torch
import numpy as np

@dataclass
class PDFSection:
    document_id: str
    page_number: int
    line_number: int
    line_text: str
    processed_date: Optional[str] = None

class OptimizedGPUPDFConverter:
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # GPU Configuration
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.cpu_count = multiprocessing.cpu_count()
        
        # Optimized batch sizes based on GPU memory
        gpu_mem = torch.cuda.get_device_properties(0).total_memory if torch.cuda.is_available() else 0
        self.batch_size = min(50000, gpu_mem // (1024 * 1024 * 10))  # Dynamically adjust based on GPU memory
        self.file_batch_size = 500  # Increased for better parallelization
        
        # Pre-compile regex patterns
        self.cleanup_pattern = re.compile(r'(\w)\1+')
        
        # Initialize GPU buffers
        if torch.cuda.is_available():
            # Preallocate GPU memory
            self.text_buffer = torch.cuda.ByteTensor(self.batch_size * 2048)
            self.workspace_buffer = torch.cuda.ByteTensor(self.batch_size * 1024)
            
            # CUDA streams for parallel processing
            self.streams = [torch.cuda.Stream() for _ in range(4)]
        
        logging.info(f"Initialized with device: {self.device}")

    def process_text_batch_gpu(self, texts: List[str]) -> List[str]:
        """Optimized GPU text processing with CUDA streams"""
        if not torch.cuda.is_available() or not texts:
            return [self.clean_text(text) for text in texts]
            
        try:
            results = []
            for stream_idx, stream in enumerate(self.streams):
                with torch.cuda.stream(stream):
                    # Process subset of texts in parallel streams
                    start_idx = stream_idx * (len(texts) // len(self.streams))
                    end_idx = (stream_idx + 1) * (len(texts) // len(self.streams))
                    batch_texts = texts[start_idx:end_idx]
                    
                    # Efficient tensor creation
                    text_tensors = [torch.tensor(list(text.encode()), dtype=torch.uint8, device=self.device) 
                                  for text in batch_texts]
                    
                    # Parallel processing using custom CUDA kernels
                    processed_tensors = []
                    for tensor in text_tensors:
                        # Remove duplicates efficiently
                        unique_mask = torch.cat([
                            torch.tensor([True], device=self.device),
                            tensor[1:] != tensor[:-1]
                        ])
                        processed = tensor[unique_mask]
                        processed_tensors.append(processed)
                    
                    # Batch decode results
                    batch_results = [tensor.cpu().numpy().tobytes().decode(errors='ignore') 
                                   for tensor in processed_tensors]
                    results.extend(batch_results)
            
            torch.cuda.synchronize()  # Ensure all streams complete
            return results
            
        except Exception as e:
            logging.error(f"GPU processing error: {e}")
            return [self.clean_text(text) for text in texts]

    async def process_pdf_batch(self, files: List[Path]) -> List[PDFSection]:
        """Enhanced parallel PDF processing"""
        async def process_single_pdf(pdf_bytes: bytes, filename: str):
            sections = []
            text_batch = []
            
            try:
                with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                    for page_number, page in enumerate(pdf.pages, 1):
                        page_text = page.extract_text()
                        if not page_text:
                            continue
                        
                        # Process text in larger batches
                        lines = page_text.split('\n')
                        text_batch.extend(lines)
                        
                        if len(text_batch) >= 5000:  # Increased batch size
                            cleaned_texts = await asyncio.to_thread(
                                self.process_text_batch_gpu, text_batch
                            )
                            sections.extend([
                                PDFSection(
                                    document_id=filename,
                                    page_number=page_number,
                                    line_number=i + len(sections) + 1,
                                    line_text=text,
                                    processed_date=datetime.now().isoformat()
                                )
                                for i, text in enumerate(cleaned_texts)
                                if text.strip()
                            ])
                            text_batch = []
                
                # Process remaining text
                if text_batch:
                    cleaned_texts = await asyncio.to_thread(
                        self.process_text_batch_gpu, text_batch
                    )
                    sections.extend([
                        PDFSection(
                            document_id=filename,
                            page_number=page_number,
                            line_number=i + len(sections) + 1,
                            line_text=text,
                            processed_date=datetime.now().isoformat()
                        )
                        for i, text in enumerate(cleaned_texts)
                        if text.strip()
                    ])
                
                return sections
                
            except Exception as e:
                logging.error(f"Error processing {filename}: {e}")
                return []

        # Parallel processing of PDFs
        pdf_data = await asyncio.gather(*[
            asyncio.to_thread(file.read_bytes)
            for file in files
        ])
        
        tasks = [
            process_single_pdf(pdf_bytes, file.stem)
            for pdf_bytes, file in zip(pdf_data, files)
        ]
        
        results = await asyncio.gather(*tasks)
        sections = []
        for result in results:
            sections.extend(result)
            
        # Clean GPU memory
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            
        return sections

    async def convert_all(self):
        """Optimized conversion process with progress tracking"""
        all_files = list(self.input_dir.glob("**/*.pdf"))
        if not all_files:
            logging.info("No PDF files found")
            return

        logging.info(f"Processing {len(all_files)} PDF files")

        with tqdm(total=len(all_files), desc="Converting PDFs") as pbar:
            for i in range(0, len(all_files), self.file_batch_size):
                batch = all_files[i:i + self.file_batch_size]
                sections = await self.process_pdf_batch(batch)
                
                if sections:
                    output_path = self.output_dir / f"batch_{i}.parquet"
                    await self.write_to_parquet(sections, output_path)
                
                pbar.update(len(batch))

        await self.merge_parquet_files()