# src/transformer.py
from __future__ import annotations
import os, json
from typing import Literal, Dict, Any
import pandas as pd

from .map_headers import map_headers
from .transform_common import row_to_ces

SourceName = Literal["NBIM", "CUSTODY"]

class Transformer:
    def __init__(self, source: SourceName):
        self.source = source

    @staticmethod
    def robust_read_csv(path: str) -> pd.DataFrame:
        for sep in [",", ";", "\\t", "|"]:
            for encoding in ["utf-8", "latin-1", "utf-16"]:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=encoding, engine="python")
                    if df.shape[1] >= 2:
                        return df
                except Exception:
                    continue
        raise RuntimeError(f"Could not read {path}")

    def transform(self, csv_path: str, out_path: str) -> int:
        df = self.robust_read_csv(csv_path)  # ← use the public helper
        df = self.preprocess_df(df)

        colmap = map_headers(list(df.columns))
        df = df.reset_index(drop=False).rename(columns={"index": "__rownum__"})

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        count = 0
        with open(out_path, "w", encoding="utf-8") as f:
            for _, row in df.iterrows():
                row_dict = self.preprocess_row(row.to_dict())
                ces, _prov = row_to_ces(row_dict, self.source, colmap)
                ces = self.postprocess_ces(ces, row_dict)
                f.write(json.dumps(ces) + "\n")
                count += 1
        return count

    def preprocess_df(self, df: pd.DataFrame) -> pd.DataFrame: return df
    def preprocess_row(self, row: Dict[str, Any]) -> Dict[str, Any]: return row
    def postprocess_ces(self, ces: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]: return ces
