"""Reversible trash executor. No permanent deletion API exists."""
from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path

PRODUCTION_ROOT=Path('/mnt/atlaspump/daily')
CONFIRMATION='TRASH_APPROVED_SOURCES'
def digest(p:Path)->str:
 h=hashlib.sha256()
 with p.open('rb') as f:
  for c in iter(lambda:f.read(1048576),b''):h.update(c)
 return h.hexdigest()
def _safe(path:Path,root:Path)->None:
 if path.is_symlink() or not path.is_absolute() or '..' in path.parts or root not in path.parents:raise ValueError('unsafe source path')
def trash(sources:list[Path],trash_root:Path,execute:bool=False,confirmation:str='',allowed_root:Path=PRODUCTION_ROOT)->list[tuple[Path,Path]]:
 plan=[]
 for source in sources:
  _safe(source,allowed_root)
  if not source.is_file():raise ValueError('source missing')
  target=trash_root/f'run={uuid.uuid4()}'/source.relative_to(allowed_root)
  if target.exists() or source.stat().st_dev!=trash_root.stat().st_dev:raise ValueError('invalid destination filesystem')
  plan.append((source,target))
 if not execute:return plan
 if confirmation!=CONFIRMATION:raise ValueError('exact confirmation required')
 moved=[]
 try:
  for s,t in plan:
   t.parent.mkdir(parents=True,exist_ok=True); before=digest(s); os.replace(s,t)
   if digest(t)!=before:raise RuntimeError('checksum changed')
   moved.append((s,t))
 except Exception:
  for s,t in reversed(moved):
   if t.exists() and not s.exists():os.replace(t,s)
  raise
 return plan
def restore(source:Path,trashed:Path,allowed_root:Path=PRODUCTION_ROOT)->None:
 _safe(source,allowed_root)
 if source.exists() or not trashed.is_file():raise ValueError('restore refused')
 if source.parent.stat().st_dev!=trashed.stat().st_dev:raise ValueError('different filesystem')
 source.parent.mkdir(parents=True,exist_ok=True);os.replace(trashed,source)
def permanent_delete(*_:object)->None:raise RuntimeError('permanent deletion is not implemented')
