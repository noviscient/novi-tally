import pandera.polars as pa


class PositionSchema(pa.DataFrameModel):
    account_id: str
    local_ccy: str
    description: str = pa.Field(nullable=True)
    bbg_yellow: str = pa.Field(nullable=True)
    quantity: int
    price: float
    asset_type: str = pa.Field(nullable=True)
    cost_price_lc: float = pa.Field(nullable=True)


class MarginSchema(pa.DataFrameModel):
    account_id: str
    liquidating_value: float
    initial_margin: float


class TradeSchema(pa.DataFrameModel):
    account_id: str
    local_ccy: str
    description: str = pa.Field(nullable=True)
    bbg_yellow: str = pa.Field(nullable=True)
    quantity: int
    price: float
