from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dcc, html, dash_table

from data_pipeline.settings import AGGREGATIONS_DIR, REJECTED_OUTPUT_DIR, ensure_directories

_ANOMALY_FILENAME = "anomaly_records.parquet"
_PREVIEW_ROW_LIMIT = 50
_TOP_REASON_LIMIT = 5


def _list_parquet_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob("*.parquet"))


def _list_csv_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob("*.csv"))


def _load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _dataframe_preview(df: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    preview = df.head(_PREVIEW_ROW_LIMIT)
    data = preview.to_dict("records")
    columns = [{"name": str(col), "id": str(col)} for col in preview.columns]
    return data, columns


def _summarise_dataframe(df: pd.DataFrame, source: Path) -> html.Div:
    column_items = [
        html.Li(f"{name}: {dtype}")
        for name, dtype in df.dtypes.items()
    ] or [html.Li("No columns present")]
    return html.Div(
        [
            html.P(f"{source.name} • {len(df):,} rows × {len(df.columns)} columns"),
            html.Details([
                html.Summary("Column dtypes"),
                html.Ul(column_items),
            ]),
        ],
    )


def _empty_message(message: str) -> html.Div:
    return html.Div(html.Em(message))


def _empty_figure(title: str):
    return {"data": [], "layout": {"title": title}}


def _default_figure(df: pd.DataFrame, title: str):
    if df.empty:
        return None
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if not numeric_cols:
        return None
    categorical_cols = df.select_dtypes(exclude="number").columns.tolist()
    numeric = numeric_cols[0]
    if categorical_cols:
        category = categorical_cols[0]
        aggregated = (
            df.groupby(category, dropna=False)[numeric]
            .sum()
            .reset_index()
            .sort_values(numeric, ascending=False)
            .head(15)
        )
        return px.bar(aggregated, x=category, y=numeric, title=f"{title}: {numeric} by {category}")
    return px.histogram(df, x=numeric, nbins=30, title=f"{title}: Distribution of {numeric}")


def _build_aggregation_options(files: Iterable[Path]) -> list[dict]:
    options = []
    for path in files:
        options.append({"label": path.stem.replace("_", " ").title(), "value": str(path)})
    return options


def _build_rejected_options(files: Iterable[Path]) -> list[dict]:
    options = []
    for path in files:
        options.append({"label": path.name, "value": str(path)})
    return options


def _reason_counts(df: pd.DataFrame, column: str, *, limit: int | None = None) -> pd.Series:
    if column not in df.columns or df.empty:
        return pd.Series(dtype="Int64")
    counts = (
        df[column]
        .fillna("UNKNOWN")
        .astype(str)
        .value_counts()
    )
    if limit is not None:
        counts = counts.head(limit)
    return counts


def _top_reason_summary(df: pd.DataFrame, column: str, heading: str) -> html.Div:
    counts = _reason_counts(df, column, limit=_TOP_REASON_LIMIT)
    if counts.empty:
        return _empty_message(f"No {heading.lower()} yet.")

    items = [
        html.Li(f"{reason}: {count:,}")
        for reason, count in counts.items()
    ]

    return html.Div(
        [
            html.Strong(heading),
            html.Ul(items),
        ],
        style={"marginBottom": "0.5rem"},
    )


def _reason_filter_options(df: pd.DataFrame, column: str) -> list[dict]:
    counts = _reason_counts(df, column)
    return [
        {"label": f"{reason} ({count:,})", "value": reason}
        for reason, count in counts.items()
    ]


def _filter_by_reason(
    df: pd.DataFrame,
    column: str,
    selected_values: Iterable[str] | None,
    valid_values: Iterable[str],
) -> pd.DataFrame:
    if column not in df.columns or not selected_values:
        return df
    allowed = set(str(value) for value in valid_values)
    chosen = [value for value in (selected_values or []) if value in allowed]
    if not chosen:
        return df
    series = df[column].fillna("UNKNOWN").astype(str)
    return df[series.isin(chosen)]


def create_app() -> Dash:
    ensure_directories()
    aggregation_files = [
        path
        for path in _list_parquet_files(AGGREGATIONS_DIR)
        if path.name != _ANOMALY_FILENAME
    ]
    rejected_files = _list_csv_files(REJECTED_OUTPUT_DIR)

    aggregation_options = _build_aggregation_options(aggregation_files)
    rejected_options = _build_rejected_options(rejected_files)

    default_aggregation = aggregation_options[0]["value"] if aggregation_options else None
    default_rejected = rejected_options[0]["value"] if rejected_options else None

    app = Dash(__name__, title="Data Pipeline Dashboard")
    app.layout = html.Div(
        [
            html.H1("Data Pipeline Dashboard"),
            html.Div(
                [
                    html.Button("Refresh data", id="refresh-data", n_clicks=0),
                    html.Span(" Data is reloaded from disk on each refresh."),
                ],
                style={"marginBottom": "1rem"},
            ),
            dcc.Tabs(
                id="dashboard-tabs",
                value="aggregations",
                children=[
                    dcc.Tab(
                        label="Aggregations Explorer",
                        value="aggregations",
                        children=[
                            html.Div(
                                [
                                    html.Label("Aggregation dataset"),
                                    dcc.Dropdown(
                                        id="aggregation-selector",
                                        options=aggregation_options,
                                        value=default_aggregation,
                                        placeholder="No aggregation parquet files found"
                                        if not aggregation_options
                                        else None,
                                        clearable=False,
                                    ),
                                ],
                                style={"maxWidth": "400px", "marginBottom": "1rem"},
                            ),
                            html.Div(id="aggregation-summary", style={"marginBottom": "1rem"}),
                            dcc.Graph(
                                id="aggregation-chart",
                                figure=_empty_figure("Select an aggregation dataset"),
                                style={"marginBottom": "1rem"},
                            ),
                            dash_table.DataTable(
                                id="aggregation-table",
                                data=[],
                                columns=[],
                                page_size=25,
                                sort_action="native",
                                filter_action="native",
                                style_table={"overflowX": "auto"},
                                style_cell={"textAlign": "left", "padding": "0.25rem"},
                            ),
                        ],
                        style={"padding": "1rem"},
                    ),
                    dcc.Tab(
                        label="Anomalies & Rejections",
                        value="anomalies",
                        children=[
                            html.H3("Anomaly Records"),
                            html.Div(id="anomaly-summary", style={"marginBottom": "1rem"}),
                            html.Div(id="anomaly-reason-summary", style={"marginBottom": "1rem"}),
                            dcc.Dropdown(
                                id="anomaly-reason-filter",
                                options=[],
                                value=[],
                                multi=True,
                                placeholder="Filter by anomaly reason",
                                clearable=True,
                                style={"maxWidth": "400px", "marginBottom": "1rem"},
                            ),
                            dash_table.DataTable(
                                id="anomaly-table",
                                data=[],
                                columns=[],
                                page_size=25,
                                sort_action="native",
                                filter_action="native",
                                style_table={"overflowX": "auto"},
                                style_cell={"textAlign": "left", "padding": "0.25rem"},
                            ),
                            html.H3("Rejected Orders", style={"marginTop": "2rem"}),
                            dcc.Dropdown(
                                id="rejected-selector",
                                options=rejected_options,
                                value=default_rejected,
                                placeholder="No rejected CSVs found",
                                clearable=False,
                            ),
                            html.Div(id="rejected-summary", style={"marginTop": "1rem"}),
                            html.Div(id="rejected-reason-summary", style={"marginTop": "1rem"}),
                            dcc.Dropdown(
                                id="rejected-reason-filter",
                                options=[],
                                value=[],
                                multi=True,
                                placeholder="Filter by rejection reason",
                                clearable=True,
                                style={"maxWidth": "400px", "marginBottom": "1rem"},
                            ),
                            dash_table.DataTable(
                                id="rejected-table",
                                data=[],
                                columns=[],
                                page_size=25,
                                sort_action="native",
                                filter_action="native",
                                style_table={"overflowX": "auto"},
                                style_cell={"textAlign": "left", "padding": "0.25rem"},
                            ),
                        ],
                        style={"padding": "1rem"},
                    ),
                ],
            ),
        ],
        style={"maxWidth": "1200px", "margin": "0 auto", "padding": "1rem"},
    )

    @app.callback(
        Output("aggregation-summary", "children"),
        Output("aggregation-table", "data"),
        Output("aggregation-table", "columns"),
        Output("aggregation-chart", "figure"),
        Input("aggregation-selector", "value"),
        Input("refresh-data", "n_clicks"),
    )
    def _update_aggregation(selected_value: str | None, _: int):
        if not selected_value:
            return _empty_message("No aggregation dataset selected."), [], [], _empty_figure("No data available")
        path = Path(selected_value)
        if not path.exists():
            message = f"{path.name} is missing. Run the aggregation pipeline."
            return _empty_message(message), [], [], _empty_figure("File not found")
        df = _load_parquet(path)
        summary = _summarise_dataframe(df, path)
        data, columns = _dataframe_preview(df)
        figure = _default_figure(df, path.stem)
        if figure is None:
            figure = _empty_figure("Add numeric columns to visualise this aggregation")
        return summary, data, columns, figure

    @app.callback(
        Output("anomaly-summary", "children"),
        Output("anomaly-reason-summary", "children"),
        Output("anomaly-reason-filter", "options"),
        Output("anomaly-table", "data"),
        Output("anomaly-table", "columns"),
        Input("anomaly-reason-filter", "value"),
        Input("refresh-data", "n_clicks"),
    )
    def _update_anomalies(selected_reasons: list[str] | None, _: int):
        anomaly_path = AGGREGATIONS_DIR / _ANOMALY_FILENAME
        if not anomaly_path.exists():
            message = _empty_message("No anomaly records parquet found yet.")
            return message, message, [], [], []
        df = _load_parquet(anomaly_path)
        summary = _summarise_dataframe(df, anomaly_path)
        reason_summary = _top_reason_summary(df, "anomaly_reason", "Top anomaly reasons")
        options = _reason_filter_options(df, "anomaly_reason")
        selected = selected_reasons or []
        filtered = _filter_by_reason(
            df,
            "anomaly_reason",
            selected,
            (option["value"] for option in options),
        )
        data, columns = _dataframe_preview(filtered)
        return summary, reason_summary, options, data, columns

    @app.callback(
        Output("rejected-summary", "children"),
        Output("rejected-reason-summary", "children"),
        Output("rejected-reason-filter", "options"),
        Output("rejected-table", "data"),
        Output("rejected-table", "columns"),
        Input("rejected-selector", "value"),
        Input("rejected-reason-filter", "value"),
        Input("refresh-data", "n_clicks"),
    )
    def _update_rejected(selected_value: str | None, selected_reasons: list[str] | None, _: int):
        if not selected_value:
            message = _empty_message("No rejected orders selected.")
            return message, message, [], [], []
        path = Path(selected_value)
        if not path.exists():
            message = f"{path.name} is missing. Run the cleaning pipeline with rejected output enabled."
            empty = _empty_message(message)
            return empty, empty, [], [], []
        df = _load_csv(path)
        summary = _summarise_dataframe(df, path)
        reason_summary = _top_reason_summary(df, "rejection_reason", "Top rejection reasons")
        options = _reason_filter_options(df, "rejection_reason")
        selected = selected_reasons or []
        filtered = _filter_by_reason(
            df,
            "rejection_reason",
            selected,
            (option["value"] for option in options),
        )
        data, columns = _dataframe_preview(filtered)
        return summary, reason_summary, options, data, columns

    return app
