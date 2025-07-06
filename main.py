import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
import hashlib
import json
from pathlib import Path
import pickle
import zlib
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#  CORE DATA STRUCTURES

@dataclass
class DataAsset:
    """Base class for all data assets"""
    id: str
    name: str
    description: str
    created_at: datetime
    updated_at: datetime
    metadata: Dict[str, Any]
    _version: int = 1
    
    def __post_init__(self):
        if not self.id:
            self.id = hashlib.sha256(f"{self.name}{datetime.now().isoformat()}".encode()).hexdigest()[:16]
    
    def bump_version(self):
        self._version += 1
        self.updated_at = datetime.now()

class DataStorage(ABC):
    """Abstract base class for different storage backends"""
    @abstractmethod
    def save(self, asset: DataAsset) -> None:
        pass
    
    @abstractmethod
    def load(self, asset_id: str) -> DataAsset:
        pass
    
    @abstractmethod
    def list_assets(self) -> List[DataAsset]:
        pass

# STORAGE IMPLEMENTATIONS

class SQLiteStorage(DataStorage):
    """SQLite implementation with compression and encryption"""
    def __init__(self, db_path: str = "data_assets.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_assets (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data BLOB
                )
            """)
    
    def save(self, asset: DataAsset) -> None:
        serialized = pickle.dumps(asset)
        compressed = zlib.compress(serialized)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO data_assets 
                (id, name, description, created_at, updated_at, metadata, version, data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                asset.id,
                asset.name,
                asset.description,
                asset.created_at.isoformat(),
                asset.updated_at.isoformat(),
                json.dumps(asset.metadata),
                asset._version,
                compressed
            ))
    
    def load(self, asset_id: str) -> DataAsset:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT data FROM data_assets WHERE id = ?", (asset_id,))
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Asset {asset_id} not found")
            
            compressed = row[0]
            serialized = zlib.decompress(compressed)
            return pickle.loads(serialized)
    
    def list_assets(self) -> List[DataAsset]:
        assets = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT data FROM data_assets")
            for row in cursor:
                compressed = row[0]
                serialized = zlib.decompress(compressed)
                assets.append(pickle.loads(serialized))
        return assets

# DATA PROCESSING

class DataPipeline:
    """Data processing pipeline with parallel execution"""
    def __init__(self, storage: DataStorage):
        self.storage = storage
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    def process_assets(self, asset_ids: List[str], processing_func) -> List[Any]:
        """Process assets in parallel"""
        futures = []
        for asset_id in asset_ids:
            asset = self.storage.load(asset_id)
            futures.append(self.executor.submit(processing_func, asset))
        
        results = [f.result() for f in futures]
        return results
    
    def analyze_assets(self, asset_ids: List[str]) -> pd.DataFrame:
        """Perform statistical analysis on multiple assets"""
        def analysis_func(asset: DataAsset) -> Dict[str, Any]:
            # Placeholder for complex analysis
            return {
                'id': asset.id,
                'name': asset.name,
                'size': len(pickle.dumps(asset)),
                'metadata_keys': len(asset.metadata)
            }
        
        results = self.process_assets(asset_ids, analysis_func)
        return pd.DataFrame(results)

# DATA MANAGER

class DataManager:
    """Main data management class"""
    def __init__(self, storage: DataStorage = None):
        self.storage = storage or SQLiteStorage()
        self.pipeline = DataPipeline(self.storage)
        self._cache = {}
    
    def add_asset(self, name: str, description: str, metadata: Dict = None) -> DataAsset:
        """Add a new data asset to the DB"""
        metadata = metadata or {}
        asset = DataAsset(
            id="",
            name=name,
            description=description,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            metadata=metadata
        )
        self.storage.save(asset)
        logger.info(f"Added new asset: {asset.id}")
        return asset
    
    def get_asset(self, asset_id: str, use_cache: bool = True) -> DataAsset:
        """Retrieve an asset with optional caching"""
        if use_cache and asset_id in self._cache:
            return self._cache[asset_id]
        
        asset = self.storage.load(asset_id)
        if use_cache:
            self._cache[asset_id] = asset
        return asset
    
    def update_asset(self, asset_id: str, updates: Dict[str, Any]) -> DataAsset:
        """Update an existing asset"""
        asset = self.get_asset(asset_id)
        
        for key, value in updates.items():
            if hasattr(asset, key):
                setattr(asset, key, value)
            else:
                asset.metadata[key] = value
        
        asset.bump_version()
        self.storage.save(asset)
        logger.info(f"Updated asset {asset_id} to version {asset._version}")
        return asset
    
    def generate_report(self) -> pd.DataFrame:
        """Generate a comprehensive report"""
        assets = self.storage.list_assets()
        if not assets:
            return pd.DataFrame()
        
        data = []
        for asset in assets:
            data.append({
                'ID': asset.id,
                'Name': asset.name,
                'Description': asset.description[:50] + '...' if len(asset.description) > 50 else asset.description,
                'Created': asset.created_at.strftime('%Y-%m-%d'),
                'Last Updated': asset.updated_at.strftime('%Y-%m-%d'),
                'Version': asset._version,
                'Metadata Items': len(asset.metadata)
            })
        
        df = pd.DataFrame(data)
        df.set_index('ID', inplace=True)
        return df

# USER INTERFACE

class DataManagerCLI:
    """Simple command-line interface"""
    def __init__(self):
        self.manager = DataManager()
    
    def run(self):
        print("Data Management Application\n")
        while True:
            print("\nOptions:")
            print("1. Add new data asset")
            print("2. View assets")
            print("3. Update asset")
            print("4. Generate report")
            print("5. Exit")
            
            choice = input("Enter your choice: ")
            
            if choice == "1":
                self._add_asset()
            elif choice == "2":
                self._view_assets()
            elif choice == "3":
                self._update_asset()
            elif choice == "4":
                self._generate_report()
            elif choice == "5":
                print("Exiting...")
                break
            else:
                print("Invalid choice, please try again.")
    
    def _add_asset(self):
        name = input("Enter asset name: ")
        description = input("Enter asset description: ")
        metadata = {}
        while True:
            key = input("Enter metadata key (or blank to finish): ")
            if not key:
                break
            value = input(f"Enter value for {key}: ")
            metadata[key] = value
        
        asset = self.manager.add_asset(name, description, metadata)
        print(f"Asset created with ID: {asset.id}")
    
    def _view_assets(self):
        assets = self.manager.storage.list_assets()
        if not assets:
            print("No assets found.")
            return
        
        for asset in assets:
            print(f"\nID: {asset.id}")
            print(f"Name: {asset.name}")
            print(f"Description: {asset.description}")
            print(f"Version: {asset._version}")
            print(f"Created: {asset.created_at.strftime('%Y-%m-%d %H:%M')}")
            print("Metadata:")
            for k, v in asset.metadata.items():
                print(f"  {k}: {v}")
    
    def _update_asset(self):
        asset_id = input("Enter asset ID to update: ")
        try:
            asset = self.manager.get_asset(asset_id)
            print(f"Current name: {asset.name}")
            new_name = input("Enter new name (or blank to keep current): ")
            
            print(f"Current description: {asset.description}")
            new_desc = input("Enter new description (or blank to keep current): ")
            
            updates = {}
            if new_name:
                updates['name'] = new_name
            if new_desc:
                updates['description'] = new_desc
            
            while True:
                key = input("Enter metadata key to update (or blank to finish): ")
                if not key:
                    break
                value = input(f"Enter new value for {key}: ")
                updates[key] = value
            
            if updates:
                self.manager.update_asset(asset_id, updates)
                print("Asset updated successfully.")
            else:
                print("No changes made.")
        except ValueError as e:
            print(f"Error: {e}")
    
    def _generate_report(self):
        report = self.manager.generate_report()
        if report.empty:
            print("No assets to report.")
        else:
            print("\nData Assets Report:")
            print(report.to_markdown())

# MAIN ENTRY POINT

if __name__ == "__main__":
    try:
        cli = DataManagerCLI()
        cli.run()
    except KeyboardInterrupt:
        print("\nApplication terminated by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print("An unexpected error occurred. Check logs for details.")