# AI / ML Layer (`ai/`)

Placeholder skeleton reserved for 1–2 future AI modules:
1. Text classification & sentiment enrichment (e.g., Laya / ModernBERT)
2. Optional multimodal/vision or volatility prediction models (e.g., media classification, XGBoost)

If GPU/CUDA dependencies (`torch`, `transformers`) conflict with `etl` later, this directory can be split into its own standalone `pyproject.toml` without disturbing `ingestion/` or `etl/`.
