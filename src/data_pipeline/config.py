"""JSON-backed configuration models for the cleaning pipeline."""

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

@dataclass
class MissingConfig:
    # strategies: "mean" | "median" | "mode" | {"constant": value}
    numeric_strategy: str = "median"
    categorical_strategy: str = "mode"
    # columns to include/exclude for imputation; empty means "auto-detect by dtype"
    include: List[str] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)

@dataclass
class DatesConfig:
    fields: List[str] = field(default_factory=list)
    # e.g. "%Y-%m-%d"; see Python datetime strftime directives
    target_format: str = "%Y-%m-%d"
    # try-parsing with these known formats first, then fall back to heuristics
    input_formats: List[str] = field(default_factory=lambda: [
        "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y",
        "%d-%b-%Y", "%d %b %Y", "%b %d, %Y",
        "%Y/%m/%d", "%d.%m.%Y"
    ])
    invalid_to_null: bool = False

@dataclass
class PriceConfig:
    fields: List[str] = field(default_factory=list)
    # normalization options
    allow_parentheses_negative: bool = True
    decimal_places: int = 2
    coerce_invalid_to_null: bool = False

@dataclass
class CategoricalConfig:
    fields: List[str] = field(default_factory=list)
    lowercase: bool = True
    strip: bool = True
    collapse_spaces: bool = True
    replace_ampersand: bool = True
    mappings: Dict[str, Dict[str, str]] = field(default_factory=dict)
    # mappings example:
    # { "category_field": { "elec.": "electronics", "elec": "electronics" } }

@dataclass
class GeoConfig:
    city_field: Optional[str] = None
    canonical_cities: List[str] = field(default_factory=list)
    city_mappings: Dict[str, str] = field(default_factory=dict)
    fuzzy_threshold: float = 0.85  # 0..1 (used by difflib)

@dataclass
class RatingsConfig:
    column: Optional[str] = None
    decimal_places: int = 1
    impute_strategy: Any = "median"  # "median" | "mean" | float

@dataclass
class BooleansConfig:
    fields: List[str] = field(default_factory=list)

@dataclass
class DeliveryConfig:
    column: Optional[str] = None
    max_days: int = 30
    clip_max: bool = True

@dataclass
class PaymentConfig:
    column: Optional[str] = None
    extra_mappings: Dict[str, str] = field(default_factory=dict)

@dataclass
class DedupConfig:
    key_fields: List[str] = field(default_factory=list)
    quantity_field: Optional[str] = None
    strategy: str = "keep_first"

@dataclass
class OutliersConfig:
    column: Optional[str] = None
    high_factor: float = 50.0
    downscale_candidates: List[int] = field(default_factory=lambda: [10, 100])
    decimal_places: int = 2

@dataclass
class PipelineConfig:
    missing: MissingConfig = field(default_factory=MissingConfig)
    dates: DatesConfig = field(default_factory=DatesConfig)
    price: PriceConfig = field(default_factory=PriceConfig)
    categorical: CategoricalConfig = field(default_factory=CategoricalConfig)
    geo: GeoConfig = field(default_factory=GeoConfig)
    ratings: RatingsConfig = field(default_factory=RatingsConfig)
    booleans: BooleansConfig = field(default_factory=BooleansConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)
    payment: PaymentConfig = field(default_factory=PaymentConfig)
    dedup: DedupConfig = field(default_factory=DedupConfig)
    outliers: OutliersConfig = field(default_factory=OutliersConfig)

def load_config(path: str) -> PipelineConfig:
    """Load a pipeline configuration from a JSON file.

    Returns a fully-populated ``PipelineConfig`` with sensible defaults for
    any missing sections.
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    def to_missing(d: dict) -> MissingConfig:
        return MissingConfig(**d) if d else MissingConfig()

    def to_dates(d: dict) -> DatesConfig:
        return DatesConfig(**d) if d else DatesConfig()

    def to_price(d: dict) -> PriceConfig:
        return PriceConfig(**d) if d else PriceConfig()

    def to_categorical(d: dict) -> CategoricalConfig:
        return CategoricalConfig(**d) if d else CategoricalConfig()

    def to_geo(d: dict) -> GeoConfig:
        return GeoConfig(**d) if d else GeoConfig()
    def to_ratings(d: dict) -> RatingsConfig:
        return RatingsConfig(**d) if d else RatingsConfig()
    def to_booleans(d: dict) -> BooleansConfig:
        return BooleansConfig(**d) if d else BooleansConfig()
    def to_delivery(d: dict) -> DeliveryConfig:
        return DeliveryConfig(**d) if d else DeliveryConfig()
    def to_payment(d: dict) -> PaymentConfig:
        return PaymentConfig(**d) if d else PaymentConfig()
    def to_dedup(d: dict) -> DedupConfig:
        return DedupConfig(**d) if d else DedupConfig()
    def to_outliers(d: dict) -> OutliersConfig:
        return OutliersConfig(**d) if d else OutliersConfig()

    return PipelineConfig(
        missing=to_missing(cfg.get("missing")),
        dates=to_dates(cfg.get("dates")),
        price=to_price(cfg.get("price")),
        categorical=to_categorical(cfg.get("categorical")),
        geo=to_geo(cfg.get("geo")),
        ratings=to_ratings(cfg.get("ratings")),
        booleans=to_booleans(cfg.get("booleans")),
        delivery=to_delivery(cfg.get("delivery")),
        payment=to_payment(cfg.get("payment")),
        dedup=to_dedup(cfg.get("dedup")),
        outliers=to_outliers(cfg.get("outliers")),
    )
