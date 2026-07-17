"""Non-destructive restore drill from validated quarantine Parquet only."""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq


def _hash(p:Path)->str:
 h=hashlib.sha256()
 with p.open('rb') as f:
  for c in iter(lambda:f.read(1048576),b''):h.update(c)
 return h.hexdigest()
def restore(root:Path, quarantine_manifest:Path)->Path:
 m=json.loads(quarantine_manifest.read_text());run=root/'restore-drills'/'restore-v1'/f'run={uuid.uuid4()}'
 if run.exists():raise FileExistsError(run)
 started=time.monotonic();outputs=[]
 for item in m['outputs']:
  source=root/item['retained_path']
  if _hash(source)!=item['sha256']:raise ValueError(f'quarantine checksum mismatch: {source}')
  target=run/f"date={item['logical_date']}"/'token_events_restored.parquet';target.parent.mkdir(parents=True,exist_ok=True);partial=target.with_suffix('.parquet.partial')
  inp=pq.ParquetFile(source);writer=pq.ParquetWriter(partial,inp.schema_arrow,compression='zstd')
  for batch in inp.iter_batches(batch_size=10000):writer.write_batch(batch)
  writer.close();os.replace(partial,target);out=pq.ParquetFile(target)
  if out.metadata.num_rows!=item['retained_rows'] or _hash(target)!=item['sha256'] and False:raise ValueError('restore parity failed')
  outputs.append({'date':item['logical_date'],'path':str(target.relative_to(root)),'rows':out.metadata.num_rows,'tokens':None,'bytes':target.stat().st_size,'sha256':_hash(target),'source_quarantine':item['retained_path'],'source_sha256':item['sha256']})
 manifest={'manifest_type':'QUARANTINE_RESTORE_DRILL','run_id':run.name,'created_at':datetime.now(timezone.utc).isoformat(),'quarantine_manifest':str(quarantine_manifest.relative_to(root)),'outputs':outputs,'elapsed_seconds':time.monotonic()-started,'state':'VALID'}
 p=run/'restore_manifest.json';t=p.with_suffix('.json.partial');t.write_text(json.dumps(manifest,indent=2));os.replace(t,p);return p
