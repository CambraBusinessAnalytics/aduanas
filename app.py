import os
from typing import Any, TypedDict, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, callback, no_update
import dash_ag_grid as dag


# -----------------------------
# CARGA DE DATOS (assets/)
# -----------------------------
def get_data() -> pd.DataFrame:
    """
    Carga la tabla agregada por puerto:
      columnas esperadas: ADUANA, kilo_neto, kilo_bruto, total, mercaderias_distintas
    Lee primero Parquet en assets/, si no existe prueba CSV.
    """
    base_parquet = os.path.join("assets", "tabla1_puertos.parquet")
    #base_csv = os.path.join("assets", "tabla1_puertos.csv")

    if os.path.exists(base_parquet):
        df = pd.read_parquet(base_parquet, engine="pyarrow")
    elif os.path.exists(base_csv):
        df = pd.read_csv(base_csv, encoding="utf-8-sig")
    else:
        # DataFrame vacío con columnas esperadas para evitar crashes
        df = pd.DataFrame(
            columns=["ADUANA", "kilo_neto", "kilo_bruto", "total", "mercaderias_distintas"]
        )

    # Tipos
    if "ADUANA" in df.columns:
        df["ADUANA"] = df["ADUANA"].astype(str)
    for c in ["kilo_neto", "kilo_bruto", "total"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "mercaderias_distintas" in df.columns:
        df["mercaderias_distintas"] = pd.to_numeric(df["mercaderias_distintas"], errors="coerce")

    # Limpieza básica
    df = df.dropna(how="all").fillna(0)
    return df


# -----------------------------
# FILTROS COMPARTIDOS
# -----------------------------
class FILTER_COMPONENT_IDS:
    port_selection = "port_selection"
    total_value_min = "total_value_min"
    total_value_max = "total_value_max"
    net_weight_min = "net_weight_min"
    net_weight_max = "net_weight_max"
    gross_weight_min = "gross_weight_min"
    gross_weight_max = "gross_weight_max"
    merchandise_count_min = "merchandise_count_min"
    merchandise_count_max = "merchandise_count_max"
    top_ports_filter = "top_ports_filter"
    port_type_filter = "port_type_filter"


FILTER_CALLBACK_INPUTS = {
    "port_selection": Input(FILTER_COMPONENT_IDS.port_selection, "value"),
    "total_value_min": Input(FILTER_COMPONENT_IDS.total_value_min, "value"),
    "total_value_max": Input(FILTER_COMPONENT_IDS.total_value_max, "value"),
    "net_weight_min": Input(FILTER_COMPONENT_IDS.net_weight_min, "value"),
    "net_weight_max": Input(FILTER_COMPONENT_IDS.net_weight_max, "value"),
    "gross_weight_min": Input(FILTER_COMPONENT_IDS.gross_weight_min, "value"),
    "gross_weight_max": Input(FILTER_COMPONENT_IDS.gross_weight_max, "value"),
    "merchandise_count_min": Input(FILTER_COMPONENT_IDS.merchandise_count_min, "value"),
    "merchandise_count_max": Input(FILTER_COMPONENT_IDS.merchandise_count_max, "value"),
    "top_ports_filter": Input(FILTER_COMPONENT_IDS.top_ports_filter, "value"),
    "port_type_filter": Input(FILTER_COMPONENT_IDS.port_type_filter, "value"),
}


def classify_port_type(port_name: str) -> str:
    p = (port_name or "").lower()
    if "aerop" in p or "airport" in p:
        return "Aeropuerto"
    if "pto" in p or "puerto" in p or "port" in p:
        return "Puerto"
    if "za" in p or "zona" in p or "frca" in p:
        return "Zona Franca"
    return "Frontera Terrestre"


def filter_data(df: pd.DataFrame, **filters) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    # Selección de puertos
    ports = filters.get("port_selection", ["all"]) or ["all"]
    if "all" not in ports:
        df = df[df["ADUANA"].isin(ports)]

    # Top N por total
    top_sel = filters.get("top_ports_filter", "all")
    if top_sel != "all":
        try:
            n = int(top_sel)
            top_ports = df.groupby("ADUANA")["total"].sum().nlargest(n).index.tolist()
            df = df[df["ADUANA"].isin(top_ports)]
        except Exception:
            pass

    # Tipo de puerto
    ptypes = filters.get("port_type_filter", ["all"]) or ["all"]
    if "all" not in ptypes:
        df["port_type"] = df["ADUANA"].apply(classify_port_type)
        df = df[df["port_type"].isin(ptypes)]

    # Rangos
    def _flt(v, lo, hi):
        return (v >= float(lo)) & (v <= float(hi))

    tmin = filters.get("total_value_min", df["total"].min())
    tmax = filters.get("total_value_max", df["total"].max())
    df = df[_flt(df["total"], tmin, tmax)]

    nmin = filters.get("net_weight_min", df["kilo_neto"].min())
    nmax = filters.get("net_weight_max", df["kilo_neto"].max())
    df = df[_flt(df["kilo_neto"], nmin, nmax)]

    bmin = filters.get("gross_weight_min", df["kilo_bruto"].min())
    bmax = filters.get("gross_weight_max", df["kilo_bruto"].max())
    df = df[_flt(df["kilo_bruto"], bmin, bmax)]

    mmin = filters.get("merchandise_count_min", df["mercaderias_distintas"].min())
    mmax = filters.get("merchandise_count_max", df["mercaderias_distintas"].max())
    df = df[
        (df["mercaderias_distintas"] >= float(mmin))
        & (df["mercaderias_distintas"] <= float(mmax))
    ]

    return df


# -----------------------------
# APP / LAYOUT (Bootstrap style)
# -----------------------------
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)
server = app.server

_df = get_data()
unique_ports = sorted(_df["ADUANA"].dropna().unique().tolist()) if not _df.empty else []
total_min, total_max = (_df["total"].min(), _df["total"].max()) if not _df.empty else (0, 0)
net_min, net_max = (_df["kilo_neto"].min(), _df["kilo_neto"].max()) if not _df.empty else (0, 0)
gross_min, gross_max = (_df["kilo_bruto"].min(), _df["kilo_bruto"].max()) if not _df.empty else (0, 0)
merc_min, merc_max = (
    (_df["mercaderias_distintas"].min(), _df["mercaderias_distintas"].max()) if not _df.empty else (0, 0)
)

port_types = sorted(list({classify_port_type(p) for p in unique_ports}))

header = html.Header(
    dbc.Container(
        dbc.Row(
            [
                dbc.Col(
                    dbc.Row(
                        [
                            dbc.Col(
                                html.Img(
                                    src="/assets/CAMBRA.png",
                                    style={"height": "80px", "width": "auto"},
                                ),
                                width="auto",
                            ),
                            dbc.Col(
                                html.H1(
                                    "Análisis de Puertos de Importación del Paraguay",
                                    className="m-0",
                                    style={
                                        "fontFamily": "Avenir, sans-serif",
                                        "fontWeight": "700",
                                        "fontSize": "2rem",
                                        "textAlign": "center",
                                        "color": "#333",
                                    },
                                ),
                                align="center",
                            ),
                        ],
                        align="center",
                    ),
                    width=10,
                ),
            ],
            justify="center",
            className="py-3",
        )
    ),
    style={"backgroundColor": "white"},
)


# Panel de filtros (acordeón)
filters_card = dbc.Card(
    dbc.CardBody(
        dbc.Accordion(
            [
                dbc.AccordionItem(
                    [
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Selección de Puerto"),
                                        dcc.Dropdown(
                                            id=FILTER_COMPONENT_IDS.port_selection,
                                            options=[{"label": "Todos", "value": "all"}]
                                            + [{"label": p, "value": p} for p in unique_ports],
                                            value=["all"],
                                            multi=True,
                                        ),
                                    ],
                                    md=4,
                                ),
                                dbc.Col(
                                    [
                                        html.Label("Top Puertos"),
                                        dcc.Dropdown(
                                            id=FILTER_COMPONENT_IDS.top_ports_filter,
                                            options=[
                                                {"label": "Todos", "value": "all"},
                                                {"label": "Top 5", "value": 5},
                                                {"label": "Top 10", "value": 10},
                                                {"label": "Top 15", "value": 15},
                                                {"label": "Top 20", "value": 20},
                                            ],
                                            value="all",
                                        ),
                                    ],
                                    md=4,
                                ),
                                dbc.Col(
                                    [
                                        html.Label("Tipo de Puerto"),
                                        dcc.Dropdown(
                                            id=FILTER_COMPONENT_IDS.port_type_filter,
                                            options=[{"label": "Todos", "value": "all"}]
                                            + [{"label": t, "value": t} for t in port_types],
                                            value=["all"],
                                            multi=True,
                                        ),
                                    ],
                                    md=4,
                                ),
                            ],
                            className="mb-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Valor Total (Gs)"),
                                        dbc.InputGroup(
                                            [
                                                dbc.Input(
                                                    id=FILTER_COMPONENT_IDS.total_value_min,
                                                    type="number",
                                                    value=float(total_min) if pd.notna(total_min) else 0,
                                                ),
                                                dbc.InputGroupText("—"),
                                                dbc.Input(
                                                    id=FILTER_COMPONENT_IDS.total_value_max,
                                                    type="number",
                                                    value=float(total_max) if pd.notna(total_max) else 0,
                                                ),
                                            ]
                                        ),
                                    ],
                                    md=4,
                                ),
                                dbc.Col(
                                    [
                                        html.Label("Peso Neto (kg)"),
                                        dbc.InputGroup(
                                            [
                                                dbc.Input(
                                                    id=FILTER_COMPONENT_IDS.net_weight_min,
                                                    type="number",
                                                    value=float(net_min) if pd.notna(net_min) else 0,
                                                ),
                                                dbc.InputGroupText("—"),
                                                dbc.Input(
                                                    id=FILTER_COMPONENT_IDS.net_weight_max,
                                                    type="number",
                                                    value=float(net_max) if pd.notna(net_max) else 0,
                                                ),
                                            ]
                                        ),
                                    ],
                                    md=4,
                                ),
                                dbc.Col(
                                    [
                                        html.Label("Peso Bruto (kg)"),
                                        dbc.InputGroup(
                                            [
                                                dbc.Input(
                                                    id=FILTER_COMPONENT_IDS.gross_weight_min,
                                                    type="number",
                                                    value=float(gross_min) if pd.notna(gross_min) else 0,
                                                ),
                                                dbc.InputGroupText("—"),
                                                dbc.Input(
                                                    id=FILTER_COMPONENT_IDS.gross_weight_max,
                                                    type="number",
                                                    value=float(gross_max) if pd.notna(gross_max) else 0,
                                                ),
                                            ]
                                        ),
                                    ],
                                    md=4,
                                ),
                            ],
                            className="mb-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Diversidad de Mercaderías"),
                                        dbc.InputGroup(
                                            [
                                                dbc.Input(
                                                    id=FILTER_COMPONENT_IDS.merchandise_count_min,
                                                    type="number",
                                                    value=float(merc_min) if pd.notna(merc_min) else 0,
                                                ),
                                                dbc.InputGroupText("—"),
                                                dbc.Input(
                                                    id=FILTER_COMPONENT_IDS.merchandise_count_max,
                                                    type="number",
                                                    value=float(merc_max) if pd.notna(merc_max) else 0,
                                                ),
                                            ]
                                        ),
                                    ],
                                    md=4,
                                ),
                                dbc.Col(html.Div(id="total_results", style={"paddingTop": 28}), md=8),
                            ]
                        ),
                    ],
                    title="Filtros",
                )
            ],
            start_collapsed=False,
        )
    ),
    className="mb-4",
)

# Tarjetas KPI (sin ddk)
kpi_cards = dbc.Row(
    [
        dbc.Col(
            dbc.Card(
                dbc.CardBody([html.H6("Cantidad de Puertos", className="text-muted"), html.H3(id="total_ports_card")])
            ),
            md=3,
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([html.H6("Peso Total (toneladas)", className="text-muted"), html.H3(id="total_weight_card")])
            ),
            md=3,
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([html.H6("Peso Promedio por Puerto (ton)", className="text-muted"), html.H3(id="avg_weight_card")])
            ),
            md=3,
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([html.H6("Puerto principal por peso", className="text-muted"), html.H3(id="top_port_card")])
            ),
            md=3,
        ),
    ],
    className="mb-4",
)

# Gráfico 1: Ranking por Valor
ranking_controls = dbc.Row(
    [
        dbc.Col(
            [
                html.Label("Cantidad de Puertos"),
                dcc.Dropdown(
                    id="ranking_top_n",
                    options=[{"label": f"Top {n}", "value": n} for n in [5, 10, 15, 20]],
                    value=10,
                    clearable=False,
                ),
            ],
            md=3,
        ),
        dbc.Col(
            [
                html.Label("Orden"),
                dcc.Dropdown(
                    id="ranking_sort_order",
                    options=[
                        {"label": "Descendente (Mayor a Menor)", "value": "desc"},
                        {"label": "Ascendente (Menor a Mayor)", "value": "asc"},
                    ],
                    value="desc",
                    clearable=False,
                ),
            ],
            md=3,
        ),
    ],
    className="mb-2",
)

ranking_card = dbc.Card(
    dbc.CardBody(
        [
            html.H5("Ranking de Puertos por Valor Total de Importación", className="card-title"),
            ranking_controls,
            dcc.Loading(dcc.Graph(id="ranking_graph"), type="circle"),
            html.Pre(id="ranking_error", style={"color": "red"}),
        ]
    ),
    className="mb-4",
)

# Gráfico 2: Scatter Volumen vs Valor (peso vs total)
scatter_controls = dbc.Row(
    [
        dbc.Col(
            [
                html.Label("Tipo de Peso"),
                dcc.Dropdown(
                    id="scatter_weight_type",
                    options=[
                        {"label": "Peso Neto (kg)", "value": "kilo_neto"},
                        {"label": "Peso Bruto (kg)", "value": "kilo_bruto"},
                    ],
                    value="kilo_neto",
                    clearable=False,
                ),
            ],
            md=3,
        ),
        dbc.Col(
            [
                html.Label("Factor de Tamaño de Puntos"),
                dcc.Slider(
                    id="scatter_size_factor",
                    min=0.1,
                    max=2.0,
                    step=0.1,
                    value=1.0,
                    marks={0.1: "0.1x", 0.5: "0.5x", 1.0: "1.0x", 1.5: "1.5x", 2.0: "2.0x"},
                    tooltip={"placement": "bottom", "always_visible": True},
                ),
            ],
            md=6,
        ),
    ],
    className="mb-2",
)

scatter_card = dbc.Card(
    dbc.CardBody(
        [
            html.H5("Volumen vs Valor Monetario de Importación", className="card-title"),
            scatter_controls,
            dcc.Loading(dcc.Graph(id="scatter_graph"), type="circle"),
            html.Pre(id="scatter_error", style={"color": "red"}),
        ]
    ),
    className="mb-4",
)

# Gráfico 3: Treemap distribución de peso (color por valor)
treemap_controls = dbc.Row(
    [
        dbc.Col(
            [
                html.Label("Tipo de Peso"),
                dcc.Dropdown(
                    id="treemap_weight_type",
                    options=[
                        {"label": "Peso Neto (kg)", "value": "kilo_neto"},
                        {"label": "Peso Bruto (kg)", "value": "kilo_bruto"},
                    ],
                    value="kilo_neto",
                    clearable=False,
                ),
            ],
            md=3,
        ),
    ],
    className="mb-2",
)

treemap_card = dbc.Card(
    dbc.CardBody(
        [
            html.H5("Distribución de Peso por Puerto", className="card-title"),
            treemap_controls,
            dcc.Loading(dcc.Graph(id="treemap_graph"), type="circle"),
            html.Pre(id="treemap_error", style={"color": "red"}),
        ]
    ),
    className="mb-4",
)

# Gráfico 4: Radar de desempeño
# Cálculo previo de top 10 por total para el selector
_top_ports = (
    _df.groupby("ADUANA")["total"].sum().nlargest(10).index.tolist() if not _df.empty else []
)
radar_controls = dbc.Row(
    [
        dbc.Col(
            [
                html.Label("Seleccionar Puertos"),
                dcc.Dropdown(
                    id="radar_ports",
                    options=[{"label": p, "value": p} for p in _top_ports],
                    value=_top_ports[:3] if len(_top_ports) >= 3 else _top_ports,
                    multi=True,
                ),
            ],
            md=6,
        ),
        dbc.Col(
            [
                html.Label("Normalizar Métricas"),
                dbc.Checklist(
                    id="radar_normalize",
                    options=[{"label": " ", "value": "enabled"}],
                    value=["enabled"],
                    switch=True,
                ),
            ],
            md=3,
        ),
    ],
    className="mb-2",
)

radar_card = dbc.Card(
    dbc.CardBody(
        [
            html.H5("Radar de Desempeño de Puertos", className="card-title"),
            radar_controls,
            dcc.Loading(dcc.Graph(id="radar_graph"), type="circle"),
            html.Pre(id="radar_error", style={"color": "red"}),
        ]
    ),
    className="mb-4",
)

# Tabla (AgGrid)
table_card = dbc.Card(
    dbc.CardBody(
        [
            html.H5("Ranking y Métricas por Puerto", className="card-title"),
            html.Div(id="data_table_title", className="mb-2", style={"fontWeight": "bold"}),
            dag.AgGrid(
                id="data_table_grid",
                columnDefs=[],
                rowData=[],
                dashGridOptions={
                    "pagination": True,
                    "paginationPageSize": 50,
                    "rowSelection": "multiple",
                    "domLayout": "normal",
                    "defaultColDef": {
                        "sortable": True,
                        "filter": True,
                        "resizable": True,
                        "floatingFilter": True,
                    },
                },
                style={"height": "600px", "width": "100%"},
            ),
            html.Small("Máximo 10.000 filas.", className="text-muted"),
        ]
    ),
    className="mb-5",
)

app.layout = html.Div(
    [
        header,
        dbc.Container(
            [
                filters_card,
                kpi_cards,
                dbc.Row([dbc.Col(ranking_card, md=12)]),
                dbc.Row([dbc.Col(scatter_card, md=12)]),
                dbc.Row([dbc.Col(treemap_card, md=12)]),
                dbc.Row([dbc.Col(radar_card, md=12)]),
                dbc.Row([dbc.Col(table_card, md=12)]),
            ],
            fluid=True,
        ),
        html.Div([
        html.P(
            "Realizado por Cambra Business Analytics. // Contacto: +595 0985 705586 // www.cambraconsultoria.com.py // email: cambraconsultoria@gmail.com",
            style={
                'font-family': 'Cambria, serif',
                'font-style': 'italic',
                'text-align': 'center',
                'color': 'white',
                'background-color': 'black',
                'margin-top': '20px',
                'width': '80%',
                'margin-left': 'auto',
                'margin-right': 'auto',
                'padding': '10px',
                'line-height': '1.5',
                'font-size': '14px'
            }
        ),
    ])
    ]
)


# -----------------------------
# CALLBACKS
# -----------------------------
@callback(Output("total_results", "children"), FILTER_CALLBACK_INPUTS)
def display_count(**kwargs):
    df = get_data()
    total = len(df)
    fdf = filter_data(df, **kwargs)
    return f"{len(fdf):,} / {total:,} filas"


@callback(
    [
        Output("total_ports_card", "children"),
        Output("total_weight_card", "children"),
        Output("avg_weight_card", "children"),
        Output("top_port_card", "children"),
    ],
    FILTER_CALLBACK_INPUTS,
)
def update_kpis(**kwargs):
    df = filter_data(get_data(), **kwargs)
    if df.empty:
        return ["—", "—", "—", "—"]

    total_ports = len(df)
    total_weight_tons = df["kilo_neto"].sum() / 1000.0
    avg_weight_tons = (total_weight_tons / total_ports) if total_ports > 0 else 0
    top_port = (
        df.loc[df["kilo_neto"].idxmax(), "ADUANA"]
        if "kilo_neto" in df and not df["kilo_neto"].isna().all()
        else "—"
    )

    return [
        f"{total_ports:,}",
        f"{total_weight_tons:,.0f}",
        f"{avg_weight_tons:,.0f}",
        (str(top_port)[:20] + "...") if len(str(top_port)) > 20 else str(top_port),
    ]
def update_ranking(top_n, sort_order, **filters) -> Tuple[go.Figure, str]:
    empty = go.Figure()
    try:
        df = filter_data(get_data(), **filters)
        if df.empty:
            empty.update_layout(
                annotations=[{"text": "Sin datos disponibles", "showarrow": False, "font": {"size": 18}}]
            )
            return empty, ""

        # Agrega por puerto
        port_totals = df.groupby("ADUANA")["total"].sum().reset_index()
        ascending = (sort_order or "desc") == "asc"
        port_totals = port_totals.sort_values("total", ascending=ascending).head(top_n or 10)

        # Reordenar para horizontal
        port_totals = port_totals.sort_values("total", ascending=True)

        port_totals["total_billones"] = port_totals["total"] / 1_000_000_000_000  # billones de Gs (10^12)

        fig = px.bar(
            port_totals,
            x="total_billones",
            y="ADUANA",
            orientation="h",
            labels={"total_billones": "Valor Total (Billones de Gs)", "ADUANA": "Aduana / Puerto"},
        )
        fig.update_traces(hovertemplate="<b>%{y}</b><br>Valor Total: %{x:.2f} billones Gs<extra></extra>")
        fig.update_layout(height=max(400, len(port_totals) * 30 + 100))

        return fig, ""
    except Exception as e:
        empty.update_layout(
            annotations=[{"text": "Error al actualizar el gráfico", "showarrow": False, "font": {"size": 18}}]
        )
        return empty, str(e)


# --- Scatter Volumen vs Valor ---
@callback(
    [Output("scatter_graph", "figure"), Output("scatter_error", "children")],
    {
        "weight_type": Input("scatter_weight_type", "value"),
        "size_factor": Input("scatter_size_factor", "value"),
        **FILTER_CALLBACK_INPUTS,
    },
)
def update_scatter(weight_type, size_factor, **filters) -> Tuple[go.Figure, str]:
    empty = go.Figure()
    try:
        df = filter_data(get_data(), **filters)
        if df.empty:
            empty.update_layout(
                annotations=[{"text": "Sin datos disponibles", "showarrow": False, "font": {"size": 18}}]
            )
            return empty, ""

        wt = weight_type or "kilo_neto"
        sf = float(size_factor or 1.0)

        # scatter por puerto (agregado)
        agg = df.groupby("ADUANA").agg(
            total=("total", "sum"),
            kilo_neto=("kilo_neto", "sum"),
            kilo_bruto=("kilo_bruto", "sum"),
            mercaderias_distintas=("mercaderias_distintas", "sum"),
        ).reset_index()

        fig = px.scatter(
            agg,
            x=wt,
            y="total",
            size=np.maximum(agg["mercaderias_distintas"] * sf, 1.0),
            hover_name="ADUANA",
            labels={
                wt: ("Peso Neto (kg)" if wt == "kilo_neto" else "Peso Bruto (kg)"),
                "total": "Valor Total (Gs)",
            },
        )
        fig.update_traces(hovertemplate="<b>%{hovertext}</b><br>Peso: %{x:,.0f} kg<br>Valor: %{y:,.0f} Gs<extra></extra>")
        fig.update_layout(xaxis_tickformat=",", yaxis_tickformat=",")

        return fig, ""
    except Exception as e:
        empty.update_layout(
            annotations=[{"text": "Error al actualizar el gráfico", "showarrow": False, "font": {"size": 18}}]
        )
        return empty, str(e)


# --- Treemap de Peso (color por valor) ---
@callback(
    [Output("treemap_graph", "figure"), Output("treemap_error", "children")],
    {"weight_type": Input("treemap_weight_type", "value"), **FILTER_CALLBACK_INPUTS},
)
def update_treemap(weight_type, **filters) -> Tuple[go.Figure, str]:
    empty = go.Figure()
    try:
        df = filter_data(get_data(), **filters)
        if df.empty:
            empty.update_layout(annotations=[{"text": "Sin datos disponibles", "showarrow": False, "font": {"size": 18}}])
            return empty, ""

        wt = weight_type or "kilo_neto"
        agg = df.groupby("ADUANA").agg(
            kilo_neto=("kilo_neto", "sum"),
            kilo_bruto=("kilo_bruto", "sum"),
            total=("total", "sum"),
            mercaderias_distintas=("mercaderias_distintas", "sum"),
        ).reset_index()

        agg["peso_millones"] = agg[wt] / 1_000_000
        agg["valor_billones"] = agg["total"] / 1_000_000_000_000

        # umbral de legibilidad (5% inferior)
        thresh = agg["peso_millones"].quantile(0.05)
        agg = agg[agg["peso_millones"] >= thresh]
        if agg.empty:
            empty.update_layout(
                annotations=[{"text": "No hay datos significativos para mostrar", "showarrow": False, "font": {"size": 18}}]
            )
            return empty, ""

        weight_label = "Peso Neto" if wt == "kilo_neto" else "Peso Bruto"
        fig = px.treemap(
            agg,
            path=["ADUANA"],
            values="peso_millones",
            color="valor_billones",
            color_continuous_scale="Viridis",
            hover_data={"peso_millones": ":.1f", "valor_billones": ":.2f", "mercaderias_distintas": ":,"},
        )
        fig.update_traces(
            hovertemplate="<b>%{label}</b><br>"
            + f"{weight_label}: %{{value:.1f}} M kg<br>"
            + "Valor Importado: %{color:.2f} billones Gs<br>"
            + "Cantidad de Mercaderías: %{customdata[2]:,}<extra></extra>",
            textinfo="label+value",
        )
        fig.update_layout(coloraxis_colorbar=dict(title="Valor Importado<br>(Billones de Gs)", title_side="right"))

        return fig, ""
    except Exception as e:
        empty.update_layout(
            annotations=[{"text": "Error al actualizar el gráfico", "showarrow": False, "font": {"size": 18}}]
        )
        return empty, str(e)


# --- Radar de Desempeño ---
@callback(
    [Output("radar_graph", "figure"), Output("radar_error", "children")],
    {"ports": Input("radar_ports", "value"), "normalize": Input("radar_normalize", "value"), **FILTER_CALLBACK_INPUTS},
)
def update_radar(ports, normalize, **filters) -> Tuple[go.Figure, str]:
    empty = go.Figure()
    try:
        df = filter_data(get_data(), **filters)
        if df.empty:
            empty.update_layout(annotations=[{"text": "Sin datos disponibles", "showarrow": False, "font": {"size": 18}}])
            return empty, ""

        selected_ports = ports or []
        if not selected_ports:
            empty.update_layout(
                annotations=[{"text": "Seleccione al menos un puerto", "showarrow": False, "font": {"size": 18}}]
            )
            return empty, ""

        f = df[df["ADUANA"].isin(selected_ports)]
        if f.empty:
            empty.update_layout(
                annotations=[{"text": "Sin datos para los puertos seleccionados", "showarrow": False, "font": {"size": 18}}]
            )
            return empty, ""

        pm = f.groupby("ADUANA").agg(
            total=("total", "sum"),
            kilo_neto=("kilo_neto", "sum"),
            kilo_bruto=("kilo_bruto", "sum"),
            mercaderias_distintas=("mercaderias_distintas", "sum"),
        ).reset_index()

        pm["eficiencia_peso"] = (pm["kilo_neto"] / pm["kilo_bruto"]).replace([np.inf, -np.inf], 0).fillna(0)
        pm["valor_por_kg"] = (pm["total"] / pm["kilo_neto"]).replace([np.inf, -np.inf], 0).fillna(0)

        metrics = ["total", "kilo_neto", "mercaderias_distintas", "eficiencia_peso", "valor_por_kg"]
        metric_labels = ["Valor Total (Gs)", "Peso Neto (kg)", "Cant. Mercaderías", "Efic. Peso", "Valor por kg (Gs/kg)"]

        norm_on = isinstance(normalize, list) and ("enabled" in normalize)
        plot_cols = []
        if norm_on:
            for m in metrics:
                col = f"{m}_norm"
                lo, hi = pm[m].min(), pm[m].max()
                pm[col] = 1.0 if lo == hi else (pm[m] - lo) / (hi - lo)
                plot_cols.append(col)
        else:
            plot_cols = metrics

        fig = go.Figure()
        for port in selected_ports:
            row = pm[pm["ADUANA"] == port]
            if row.empty:
                continue
            vals = [row[c].iloc[0] for c in plot_cols]
            vals.append(vals[0])  # cerrar el polígono
            fig.add_trace(
                go.Scatterpolar(r=vals, theta=metric_labels + [metric_labels[0]], fill="toself", name=port, opacity=0.7)
            )

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1] if norm_on else None)),
            showlegend=True,
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
        )

        return fig, ""
    except Exception as e:
        empty.update_layout(
            annotations=[{"text": "Error al actualizar el gráfico", "showarrow": False, "font": {"size": 18}}]
        )
        return empty, str(e)


# --- Tabla (AgGrid) ---
@callback(
    [Output("data_table_grid", "columnDefs"), Output("data_table_grid", "rowData"), Output("data_table_title", "children")],
    FILTER_CALLBACK_INPUTS,
)
def update_table(**filters):
    try:
        df = filter_data(get_data(), **filters)
        if df.empty:
            return [], [], "Sin datos"

        # limit
        df = df.head(10_000)

        # Agrega por puerto
        port_rankings = (
            df.groupby("ADUANA")
            .agg(total=("total", "sum"), kilo_neto=("kilo_neto", "sum"), kilo_bruto=("kilo_bruto", "sum"), mercaderias_distintas=("mercaderias_distintas", "sum"))
            .reset_index()
        )

        port_rankings["total_rank"] = port_rankings["total"].rank(method="dense", ascending=False).astype(int)
        port_rankings["weight_rank"] = port_rankings["kilo_neto"].rank(method="dense", ascending=False).astype(int)
        port_rankings["diversity_rank"] = port_rankings["mercaderias_distintas"].rank(method="dense", ascending=False).astype(int)

        port_rankings = port_rankings.sort_values("total", ascending=False)

        column_defs = [
            {"headerName": "Ranking (por Valor)", "field": "total_rank", "type": "numericColumn", "filter": "agNumberColumnFilter", "width": 150, "pinned": "left"},
            {"headerName": "Aduana / Puerto", "field": "ADUANA", "filter": "agTextColumnFilter", "width": 220, "pinned": "left"},
            {"headerName": "Valor Total (Gs)", "field": "total", "type": "numericColumn", "filter": "agNumberColumnFilter", "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}, "width": 170},
            {"headerName": "Peso Neto (kg)", "field": "kilo_neto", "type": "numericColumn", "filter": "agNumberColumnFilter", "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}, "width": 170},
            {"headerName": "Peso Bruto (kg)", "field": "kilo_bruto", "type": "numericColumn", "filter": "agNumberColumnFilter", "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}, "width": 170},
            {"headerName": "Diversidad de Mercaderías", "field": "mercaderias_distintas", "type": "numericColumn", "filter": "agNumberColumnFilter", "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}, "width": 210},
            {"headerName": "Ranking por Peso", "field": "weight_rank", "type": "numericColumn", "filter": "agNumberColumnFilter", "width": 160},
            {"headerName": "Ranking por Diversidad", "field": "diversity_rank", "type": "numericColumn", "filter": "agNumberColumnFilter", "width": 180},
        ]

        df_cleaned = port_rankings.copy()
        for col in df_cleaned.columns:
            if pd.api.types.is_numeric_dtype(df_cleaned[col]):
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors="coerce")

        row_data = df_cleaned.replace({np.nan: None}).to_dict("records")

        title_text = f"Mostrando {len(row_data)} puertos (agregado por aduana)"
        return column_defs, row_data, title_text

    except Exception as e:
        return [], [], f"Error: {e}"


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=int(os.environ.get("PORT", 8050)), debug=False)
