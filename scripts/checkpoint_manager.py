"""
Checkpointing system for Pipeline 2.
Saves progress every N sections and resumes from last checkpoint.
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path

class CheckpointManager:
    """Manages checkpoints for Pipeline 2 extraction."""
    
    def __init__(self, checkpoint_dir="data/processed/pipeline2_checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
    def get_checkpoint_file(self, title):
        """Get checkpoint file path for a title."""
        safe_title = title.replace(" ", "_").replace("/", "_")
        return self.checkpoint_dir / f"{safe_title}_checkpoint.json"
    
    def load_checkpoint(self, title):
        """Load checkpoint for a title. Returns (processed_ids, results) or (set(), [])."""
        checkpoint_file = self.get_checkpoint_file(title)
        
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data.get("processed_ids", [])), data.get("results", [])
        
        return set(), []
    
    def save_checkpoint(self, title, processed_ids, results, metadata=None):
        """Save checkpoint for a title."""
        checkpoint_file = self.get_checkpoint_file(title)
        
        data = {
            "title": title,
            "timestamp": datetime.now().isoformat(),
            "total_processed": len(processed_ids),
            "processed_ids": list(processed_ids),
            "results": results,
            "metadata": metadata or {}
        }
        
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return checkpoint_file
    
    def get_all_checkpoints(self):
        """List all available checkpoints."""
        checkpoints = []
        for f in self.checkpoint_dir.glob("*_checkpoint.json"):
            with open(f, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
                checkpoints.append({
                    "title": data.get("title"),
                    "file": str(f),
                    "processed": data.get("total_processed", 0),
                    "timestamp": data.get("timestamp")
                })
        return checkpoints
    
    def clear_checkpoint(self, title):
        """Delete checkpoint for a title."""
        checkpoint_file = self.get_checkpoint_file(title)
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            return True
        return False


class ExtractionTracker:
    """Tracks extraction progress with detailed statistics."""
    
    def __init__(self):
        self.results = []
        self.errors = []
        self.start_time = None
        self.section_times = []
        
    def start(self):
        """Start tracking."""
        self.start_time = time.time()
        
    def record_result(self, section_id, success, time_taken, tokens_used=0):
        """Record a single extraction result."""
        self.section_times.append(time_taken)
        
        if success:
            self.results.append({
                "section_id": section_id,
                "time": time_taken,
                "tokens": tokens_used
            })
        else:
            self.errors.append({
                "section_id": section_id,
                "time": time_taken,
                "error": "extraction_failed"
            })
    
    def get_stats(self):
        """Get current statistics."""
        if not self.section_times:
            return {}
        
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        return {
            "total_processed": len(self.results) + len(self.errors),
            "successful": len(self.results),
            "errors": len(self.errors),
            "error_rate": len(self.errors) / (len(self.results) + len(self.errors)) * 100 if (len(self.results) + len(self.errors)) > 0 else 0,
            "elapsed_time": elapsed,
            "avg_time_per_section": sum(self.section_times) / len(self.section_times) if self.section_times else 0,
            "throughput": (len(self.results) + len(self.errors)) / elapsed if elapsed > 0 else 0,
            "estimated_remaining": 0  # Will be calculated based on total
        }
    
    def print_progress(self, total_sections, interval=100):
        """Print progress update."""
        stats = self.get_stats()
        
        if stats["total_processed"] % interval == 0 and stats["total_processed"] > 0:
            remaining = total_sections - stats["total_processed"]
            eta_seconds = remaining / stats["throughput"] if stats["throughput"] > 0 else 0
            eta_hours = eta_seconds / 3600
            
            print(f"\nProgress: {stats['total_processed']}/{total_sections} "
                  f"({stats['total_processed']/total_sections*100:.1f}%)")
            print(f"  Successful: {stats['successful']}, Errors: {stats['errors']} "
                  f"({stats['error_rate']:.1f}% error rate)")
            print(f"  Throughput: {stats['throughput']:.2f} req/s")
            print(f"  ETA: {eta_hours:.1f} hours")


# Demo usage
if __name__ == "__main__":
    print("=" * 70)
    print("CHECKPOINTING SYSTEM DEMO")
    print("=" * 70)
    
    # Create checkpoint manager
    cm = CheckpointManager()
    
    # Simulate some extractions
    print("\nSimulating extractions...")
    
    title = "Test Title"
    processed_ids = set()
    results = []
    
    for i in range(10):
        section_id = f"section_{i}"
        processed_ids.add(section_id)
        results.append({
            "section_id": section_id,
            "extracted_fields": {"field1": f"value_{i}"},
            "confidence": 0.95
        })
        
        # Save checkpoint every 5 sections
        if (i + 1) % 5 == 0:
            cm.save_checkpoint(title, processed_ids, results)
            print(f"  Saved checkpoint at {i+1} sections")
    
    # Load and verify checkpoint
    loaded_ids, loaded_results = cm.load_checkpoint(title)
    print(f"\nLoaded checkpoint: {len(loaded_ids)} sections, {len(loaded_results)} results")
    
    # List all checkpoints
    checkpoints = cm.get_all_checkpoints()
    print(f"\nAll checkpoints: {len(checkpoints)}")
    for cp in checkpoints:
        print(f"  {cp['title']}: {cp['processed']} sections ({cp['timestamp']})")
    
    print("\n" + "=" * 70)
    print("CHECKPOINTING SYSTEM READY")
    print("=" * 70)
