from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, State, dcc, html, dash_table
from dash.exceptions import PreventUpdate

from data_pipeline.settings import AGGREGATIONS_DIR, REJECTED_OUTPUT_DIR, ensure_directories

_ANOMALY_FILENAME = "anomaly_records.parquet"
_TOP_PRODUCTS_BY_CATEGORY_FILENAME = "top_products_by_category.parquet"
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


def _create_top_categories_figure(df: pd.DataFrame):
    """Create a custom figure for top_categories with human-friendly titles (default: discount)"""
    return _create_dynamic_top_categories_figure(df, "avg_discount_percent")


def _create_dynamic_top_categories_figure(df: pd.DataFrame, metric: str):
    """Create a top_categories figure with dynamic metric selection"""
    if df.empty or metric not in df.columns or "category" not in df.columns:
        return None
        
    # Define metric configurations
    metric_config = {
        "avg_discount_percent": {
            "title": "Top Categories by Average Discount",
            "label": "Average Discount %",
            "hover_data": ["total_revenue", "order_count"]
        },
        "total_revenue": {
            "title": "Top Categories by Revenue", 
            "label": "Total Revenue",
            "hover_data": ["avg_discount_percent", "order_count"]
        },
        "order_count": {
            "title": "Top Categories by Order Volume",
            "label": "Order Count", 
            "hover_data": ["avg_discount_percent", "total_revenue"]
        }
    }
    
    config = metric_config.get(metric, {
        "title": f"Top Categories by {metric}",
        "label": metric,
        "hover_data": []
    })
    
    # Sort by the selected metric and show top 15
    top_categories = df.sort_values(metric, ascending=False).head(15)
    
    return px.bar(
        top_categories, 
        x="category", 
        y=metric,
        title=f"{config['title']} (Click to view category products)",
        labels={"category": "Category", metric: config["label"]},
        hover_data=config["hover_data"]
    )


def _create_top_products_by_category_figure(df: pd.DataFrame, metric: str):
    """Create a top_products_by_category figure with dynamic metric selection"""
    if df.empty or "category" not in df.columns:
        return None
        
    # Define metric configurations
    metric_config = {
        "total_revenue": {
            "title": "Categories by Total Revenue",
            "label": "Total Revenue",
            "hover_data": ["order_count"]
        },
        "total_quantity": {
            "title": "Categories by Total Quantity", 
            "label": "Total Quantity",
            "hover_data": ["order_count"]
        },
        "order_count": {
            "title": "Categories by Order Count",
            "label": "Order Count", 
            "hover_data": ["total_revenue"]
        }
    }
    
    config = metric_config.get(metric, {
        "title": f"Categories by {metric}",
        "label": metric,
        "hover_data": []
    })
    
    # Sum by category and sort descending
    if metric not in df.columns:
        return None
    category_metrics = df.groupby("category")[metric].sum().reset_index()
    category_metrics = category_metrics.sort_values(metric, ascending=False).head(15)
    
    return px.bar(
        category_metrics, 
        x="category", 
        y=metric,
        title=config['title'],
        labels={"category": "Category", metric: config["label"]}
    )


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
                            html.Div(
                                [
                                    html.Label("Chart Metric"),
                                    dcc.RadioItems(
                                        id="chart-metric-selector",
                                        options=[
                                            {"label": "Average Discount %", "value": "avg_discount_percent"},
                                            {"label": "Total Revenue", "value": "total_revenue"},
                                            {"label": "Order Count", "value": "order_count"},
                                        ],
                                        value="total_revenue",
                                        inline=True,
                                    ),
                                ],
                                id="chart-metric-controls",
                                style={"display": "none", "marginBottom": "1rem"},
                            ),
                            html.Div(id="aggregation-summary", style={"marginBottom": "1rem"}),
                            dcc.Graph(
                                id="aggregation-chart",
                                figure=_empty_figure("Select an aggregation dataset"),
                                style={"marginBottom": "1rem"},
                            ),
                            html.Div(
                                [
                                    html.H3("Category best sellers"),
                                    html.Div(
                                        [
                                            html.Label("Metric"),
                                            dcc.RadioItems(
                                                id="category-metric",
                                                options=[
                                                    {"label": "Revenue", "value": "revenue"},
                                                    {"label": "Units sold", "value": "units"},
                                                ],
                                                value="revenue",
                                                inline=True,
                                            ),
                                        ],
                                        style={"marginBottom": "0.75rem"},
                                    ),
                                    html.Label("Category"),
                                    dcc.Dropdown(
                                        id="category-selector",
                                        options=[],
                                        value=None,
                                        placeholder="Run build-aggregations to populate category rankings",
                                        clearable=False,
                                        style={"maxWidth": "400px", "marginBottom": "1rem"},
                                    ),
                                    html.Div(
                                        [
                                            html.Label("Top N products"),
                                            dcc.Input(
                                                id="category-top-n",
                                                type="number",
                                                min=1,
                                                max=50,
                                                step=1,
                                                value=5,
                                                style={"width": "120px"},
                                            ),
                                            html.Span(
                                                "  Applies when a specific category is selected",
                                                style={"marginLeft": "0.5rem", "fontStyle": "italic", "fontSize": "0.85rem"},
                                            ),
                                        ],
                                        style={"marginBottom": "1rem"},
                                    ),
                                    dcc.Graph(
                                        id="category-best-chart",
                                        figure=_empty_figure("Select a category to see top products chart"),
                                        style={"marginBottom": "1rem"},
                                    ),
                                    dash_table.DataTable(
                                        id="category-best-table",
                                        data=[],
                                        columns=[],
                                        page_size=20,
                                        sort_action="native",
                                        filter_action="native",
                                        style_table={"overflowX": "auto"},
                                        style_cell={"textAlign": "left", "padding": "0.25rem"},
                                    ),
                                ],
                                id="top-products-category",
                                style={"display": "none", "marginBottom": "1rem"},
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
        Output("top-products-category", "style"),
        Output("chart-metric-controls", "style"),
        Input("aggregation-selector", "value"),
        Input("refresh-data", "n_clicks"),
    )
    def _update_aggregation(selected_value: str | None, _: int):
        hidden_style = {"display": "none"}
        category_style = hidden_style
        chart_metric_style = hidden_style

        if not selected_value:
            message = _empty_message("No aggregation dataset selected.")
            return message, [], [], _empty_figure("No data available"), category_style, chart_metric_style

        path = Path(selected_value)
        if not path.exists():
            missing_message = f"{path.name} is missing. Run the aggregation pipeline."
            return (
                _empty_message(missing_message),
                [],
                [],
                _empty_figure("File not found"),
                category_style,
                chart_metric_style,
            )

        df = _load_parquet(path)
        summary = _summarise_dataframe(df, path)
        data, columns = _dataframe_preview(df)
        
        # Create figure with custom handling for specific files
        if path.name == "top_categories.parquet":
            figure = _create_top_categories_figure(df)
        elif path.name == _TOP_PRODUCTS_BY_CATEGORY_FILENAME:
            # Use default metric (total_revenue) for initial load
            figure = _create_top_products_by_category_figure(df, "total_revenue")
        else:
            figure = _default_figure(df, path.stem)
            
        if figure is None:
            figure = _empty_figure("Add numeric columns to visualise this aggregation")

        if path.name == _TOP_PRODUCTS_BY_CATEGORY_FILENAME:
            category_style = {"marginBottom": "1rem"}
            
        # Show chart metric controls for files that support multiple metrics
        if path.name in ["top_categories.parquet", _TOP_PRODUCTS_BY_CATEGORY_FILENAME]:
            chart_metric_style = {"marginBottom": "1rem"}

        return summary, data, columns, figure, category_style, chart_metric_style

    @app.callback(
        Output("aggregation-chart", "figure", allow_duplicate=True),
        Input("chart-metric-selector", "value"),
        State("aggregation-selector", "value"),
        prevent_initial_call=True,
    )
    def _update_chart_metric(selected_metric, current_aggregation):
        if not current_aggregation or not selected_metric:
            raise PreventUpdate
            
        path = Path(current_aggregation)
        if not path.exists():
            raise PreventUpdate
            
        df = _load_parquet(path)
        if df.empty:
            raise PreventUpdate
            
        # Handle different file types
        if path.name == "top_categories.parquet":
            if selected_metric not in df.columns:
                raise PreventUpdate
            figure = _create_dynamic_top_categories_figure(df, selected_metric)
        elif path.name == _TOP_PRODUCTS_BY_CATEGORY_FILENAME:
            figure = _create_top_products_by_category_figure(df, selected_metric)
        else:
            raise PreventUpdate
            
        return figure if figure else _empty_figure("Unable to create chart")

    @app.callback(
        Output("category-selector", "options"),
        Output("category-selector", "value"),
        Input("aggregation-selector", "value"),
        Input("refresh-data", "n_clicks"),
        State("category-selector", "value"),
    )
    def _update_category_selector(selected_value: str | None, _: int, current_value: str | None):
        if not selected_value:
            return [], None
        path = Path(selected_value)
        if path.name != _TOP_PRODUCTS_BY_CATEGORY_FILENAME or not path.exists():
            return [], None
        df = _load_parquet(path)
        if df.empty or "category" not in df.columns:
            return [], None
        categories = sorted(df["category"].dropna().astype(str).unique().tolist())
        if not categories:
            return [], None
        options = [{"label": category, "value": category} for category in categories]
        value = current_value if current_value in categories else categories[0]
        return options, value

    @app.callback(
        Output("category-best-table", "data"),
        Output("category-best-table", "columns"),
        Output("category-best-chart", "figure"),
        Input("category-selector", "value"),
        Input("category-metric", "value"),
        Input("category-top-n", "value"),
        Input("aggregation-selector", "value"),
        Input("refresh-data", "n_clicks"),
    )
    def _update_category_table(
        selected_category: str | None,
        selected_metric: str | None,
        top_n: int | None,
        selected_value: str | None,
        _: int,
    ):
        if not selected_value:
            return [], [], _empty_figure("No data selected")
        path = Path(selected_value)
        if path.name != _TOP_PRODUCTS_BY_CATEGORY_FILENAME or not path.exists():
            return [], [], _empty_figure("Top products by category file not found")
        df = _load_parquet(path)
        if df.empty:
            return [], [], _empty_figure("No data available")
        metric = selected_metric if selected_metric in {"revenue", "units"} else "revenue"
        metric_df = df[df["metric_type"] == metric].copy()
        if metric_df.empty:
            return [], [], _empty_figure(f"No {metric} data available")
        metric_df = metric_df.sort_values(["category", "rank"]).reset_index(drop=True)

        DEFAULT_PER_CATEGORY = 3
        if selected_category:
            filtered = metric_df[metric_df["category"].astype(str) == selected_category]
            limit = DEFAULT_PER_CATEGORY
            if isinstance(top_n, (int, float)):
                try:
                    limit = max(1, int(top_n))
                except (TypeError, ValueError):
                    limit = DEFAULT_PER_CATEGORY
            filtered = filtered.head(limit)
        else:
            filtered = metric_df.groupby("category", group_keys=False).head(DEFAULT_PER_CATEGORY)

        # Create chart
        if filtered.empty:
            figure = _empty_figure("No data to display")
        else:
            # Determine what to display on y-axis based on metric
            y_column = "total_revenue" if metric == "revenue" else "total_quantity"
            y_label = "Total Revenue" if metric == "revenue" else "Total Quantity"
            
            # Create bar chart
            figure = px.bar(
                filtered,
                x="product_name",
                y=y_column,
                color="category" if not selected_category else None,
                title=f"Top Products by {y_label}" + (f" - {selected_category}" if selected_category else ""),
                text_auto=True,
                labels={
                    "product_name": "Product",
                    y_column: y_label,
                    "category": "Category"
                }
            )
            figure.update_layout(
                xaxis_tickangle=-45,
                margin=dict(b=100),  # Add bottom margin for rotated labels
                showlegend=not selected_category  # Only show legend when multiple categories
            )
        
        data = filtered.to_dict("records")
        columns = [{"name": str(col), "id": str(col)} for col in filtered.columns]
        return data, columns, figure

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

    @app.callback(
        Output("aggregation-selector", "value"),
        Output("category-selector", "value", allow_duplicate=True),
        Input("aggregation-chart", "clickData"),
        State("aggregation-selector", "value"),
        prevent_initial_call=True,
    )
    def _handle_chart_click(click_data, current_aggregation):
        if not click_data or not current_aggregation:
            raise PreventUpdate
            
        # Only handle clicks on top_categories charts
        current_path = Path(current_aggregation)
        if current_path.name != "top_categories.parquet":
            raise PreventUpdate
            
        # Extract category from click data
        try:
            # For bar charts, the category is typically in the x value
            clicked_category = click_data["points"][0]["x"]
            
            # Find the top_products_by_category file
            top_products_by_category_path = None
            for path in _list_parquet_files(AGGREGATIONS_DIR):
                if path.name == _TOP_PRODUCTS_BY_CATEGORY_FILENAME:
                    top_products_by_category_path = str(path)
                    break
            
            if not top_products_by_category_path:
                raise PreventUpdate
                
            return top_products_by_category_path, clicked_category
            
        except (KeyError, IndexError, TypeError):
            # If we can't extract the category, don't update
            raise PreventUpdate

    return app
