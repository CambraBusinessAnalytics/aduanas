import os
import pandas as pd
from logger import logger
from flask_caching import Cache

# Configurar caché
cache = Cache()

# Definir ruta base para assets
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(BASE_DIR, "assets")

@cache.memoize()
def get_data(filename="tabla1_puertos.parquet"):
    """
    Carga los datos de la tabla1_puertos desde la carpeta assets.
    Soporta archivos parquet o csv.
    """
    data_path = os.path.join(ASSETS_DIR, filename)

    # Definir tipos de columnas esperadas
    type_mapping = {
        "ADUANA": str,
        "kilo_neto": "Float64",
        "kilo_bruto": "Float64", 
        "total": "Float64",
        "mercaderias_distintas": "Int64",
    }

    if data_path.endswith(".parquet"):
        df = pd.read_parquet(data_path, engine="pyarrow")
    else:
        df = pd.read_csv(
            data_path,
            dtype=type_mapping,
            sep=None,
            engine="python",
            encoding="utf-8-sig"
        )

    logger.debug("Datos cargados desde %s. Shape: %s", filename, df.shape)
    logger.debug("Muestra de datos:\n%s", df.head())

    # Filtrar filas completamente vacías
    df = df.dropna(how="all")

    logger.debug("Datos después de filtrar nulos. Shape: %s", df.shape)

    return df

from dash import Dash

app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server   



import dash_design_kit as ddk
from dash import html

component_registry = {}  # Map of component names to their instances
failed_components = {}  # Map of component names to their error messages

### Component Imports
try:
    from components.filter_component import component as filter_component
except Exception:
    filter_component = None

try:
    from components.data_cards import component as data_card_component
except Exception:
    data_card_component = None

try:
    from components.data_table import component as data_table_component
except Exception:
    data_table_component = None

try:
    from components.port_ranking_value import component as port_ranking_value_component
    component_registry["port_ranking_value"] = port_ranking_value_component
except Exception as e:
    failed_components["port_ranking_value"] = str(e)
    print(f"[COMPONENT FAILURE] Failed to import port_ranking_value:", e)

try:
    from components.volume_vs_value_scatter import component as volume_vs_value_scatter_component
    component_registry["volume_vs_value_scatter"] = volume_vs_value_scatter_component
except Exception as e:
    failed_components["volume_vs_value_scatter"] = str(e)
    print(f"[COMPONENT FAILURE] Failed to import volume_vs_value_scatter:", e)

try:
    from components.merchandise_diversity_ranking import component as merchandise_diversity_ranking_component
    component_registry["merchandise_diversity_ranking"] = merchandise_diversity_ranking_component
except Exception as e:
    failed_components["merchandise_diversity_ranking"] = str(e)
    print(f"[COMPONENT FAILURE] Failed to import merchandise_diversity_ranking:", e)

try:
    from components.weight_comparison_treemap import component as weight_comparison_treemap_component
    component_registry["weight_comparison_treemap"] = weight_comparison_treemap_component
except Exception as e:
    failed_components["weight_comparison_treemap"] = str(e)
    print(f"[COMPONENT FAILURE] Failed to import weight_comparison_treemap:", e)

try:
    from components.port_performance_radar import component as port_performance_radar_component
    component_registry["port_performance_radar"] = port_performance_radar_component
except Exception as e:
    failed_components["port_performance_radar"] = str(e)
    print(f"[COMPONENT FAILURE] Failed to import port_performance_radar:", e)


def component(preview=False):
    layout_items = []

    # Add hero section
    app_title = "Análisis de Puertos de Importación del Paraguay"
    app_description = "Aplicación interactiva que analiza la actividad de importación en las aduanas del Paraguay, mostrando rankings y comparaciones de volumen físico, valor monetario y diversidad de mercaderías por puerto."
    app_logo = "/assets/cambra_logo.png"
    app_tags = [
    ddk.Tag(text="**Datos actualizados:** 19 de septiembre de 2025", icon="circle-check"),
    ddk.Tag(text="**Fuente:** Dirección Nacional de Aduanas", icon="database"),
    ddk.Tag(text="**Elaborado por Cambra Consultoría Analítica con Plotly Dash**", icon="user"),
]


    layout_items.append(
        ddk.Hero(title=app_title, description=app_description, logo=app_logo, tags=app_tags)
    )

    if filter_component:
        layout_items.append(filter_component()["layout"])

    if data_card_component:
        layout_items.append(data_card_component()["layout"])

    if "port_ranking_value" in component_registry:
        try:
            port_ranking_value_layout = component_registry["port_ranking_value"]()["layout"]
        except Exception as e:
            port_ranking_value_layout = ddk.Card(
                children=[
                    ddk.CardHeader(
                        title=f'Error in port_ranking_value: {e}',
                        style={"color": "red"},
                    ),
                    ddk.Graph()
                ]
            )
        
        port_ranking_value_layout.width = 50
        if preview:
            port_ranking_value_layout.children[0].children = [
                html.Button(
                    children=[
                        ddk.Icon(icon_name="pencil", id="edit-icon"),
                        "Edit",
                    ],
                    style={
                        "color": "var(--button_background_color)",
                        "background-color": "transparent",
                        "border-width": "1px",
                        "gap": "5px",
                        "display": "flex",
                        "align-items": "center",
                        "padding": "5px 10px",
                    },
                    id={"type": "edit-component-button", "index": "port_ranking_value"},
                )
            ]
        layout_items.append(port_ranking_value_layout)

    if "volume_vs_value_scatter" in component_registry:
        try:
            volume_vs_value_scatter_layout = component_registry["volume_vs_value_scatter"]()["layout"]
        except Exception as e:
            volume_vs_value_scatter_layout = ddk.Card(
                children=[
                    ddk.CardHeader(
                        title=f'Error in volume_vs_value_scatter: {e}',
                        style={"color": "red"},
                    ),
                    ddk.Graph()
                ]
            )
        
        volume_vs_value_scatter_layout.width = 50
        if preview:
            volume_vs_value_scatter_layout.children[0].children = [
                html.Button(
                    children=[
                        ddk.Icon(icon_name="pencil", id="edit-icon"),
                        "Edit",
                    ],
                    style={
                        "color": "var(--button_background_color)",
                        "background-color": "transparent",
                        "border-width": "1px",
                        "gap": "5px",
                        "display": "flex",
                        "align-items": "center",
                        "padding": "5px 10px",
                    },
                    id={"type": "edit-component-button", "index": "volume_vs_value_scatter"},
                )
            ]
        layout_items.append(volume_vs_value_scatter_layout)

    if "merchandise_diversity_ranking" in component_registry:
        try:
            merchandise_diversity_ranking_layout = component_registry["merchandise_diversity_ranking"]()["layout"]
        except Exception as e:
            merchandise_diversity_ranking_layout = ddk.Card(
                children=[
                    ddk.CardHeader(
                        title=f'Error in merchandise_diversity_ranking: {e}',
                        style={"color": "red"},
                    ),
                    ddk.Graph()
                ]
            )
        
        merchandise_diversity_ranking_layout.width = 50
        if preview:
            merchandise_diversity_ranking_layout.children[0].children = [
                html.Button(
                    children=[
                        ddk.Icon(icon_name="pencil", id="edit-icon"),
                        "Edit",
                    ],
                    style={
                        "color": "var(--button_background_color)",
                        "background-color": "transparent",
                        "border-width": "1px",
                        "gap": "5px",
                        "display": "flex",
                        "align-items": "center",
                        "padding": "5px 10px",
                    },
                    id={"type": "edit-component-button", "index": "merchandise_diversity_ranking"},
                )
            ]
        layout_items.append(merchandise_diversity_ranking_layout)

    if "weight_comparison_treemap" in component_registry:
        try:
            weight_comparison_treemap_layout = component_registry["weight_comparison_treemap"]()["layout"]
        except Exception as e:
            weight_comparison_treemap_layout = ddk.Card(
                children=[
                    ddk.CardHeader(
                        title=f'Error in weight_comparison_treemap: {e}',
                        style={"color": "red"},
                    ),
                    ddk.Graph()
                ]
            )
        
        weight_comparison_treemap_layout.width = 50
        if preview:
            weight_comparison_treemap_layout.children[0].children = [
                html.Button(
                    children=[
                        ddk.Icon(icon_name="pencil", id="edit-icon"),
                        "Edit",
                    ],
                    style={
                        "color": "var(--button_background_color)",
                        "background-color": "transparent",
                        "border-width": "1px",
                        "gap": "5px",
                        "display": "flex",
                        "align-items": "center",
                        "padding": "5px 10px",
                    },
                    id={"type": "edit-component-button", "index": "weight_comparison_treemap"},
                )
            ]
        layout_items.append(weight_comparison_treemap_layout)

    if "port_performance_radar" in component_registry:
        try:
            port_performance_radar_layout = component_registry["port_performance_radar"]()["layout"]
        except Exception as e:
            port_performance_radar_layout = ddk.Card(
                children=[
                    ddk.CardHeader(
                        title=f'Error in port_performance_radar: {e}',
                        style={"color": "red"},
                    ),
                    ddk.Graph()
                ]
            )
        
        port_performance_radar_layout.width = 50
        if preview:
            port_performance_radar_layout.children[0].children = [
                html.Button(
                    children=[
                        ddk.Icon(icon_name="pencil", id="edit-icon"),
                        "Edit",
                    ],
                    style={
                        "color": "var(--button_background_color)",
                        "background-color": "transparent",
                        "border-width": "1px",
                        "gap": "5px",
                        "display": "flex",
                        "align-items": "center",
                        "padding": "5px 10px",
                    },
                    id={"type": "edit-component-button", "index": "port_performance_radar"},
                )
            ]
        layout_items.append(port_performance_radar_layout)

    if data_table_component:
        layout_items.append(data_table_component()["layout"])

    return layout_items

# Generated by Plotly Studio

theme = {
    "accent":"#D4AF37",
    "accent_positive":"#2E8B57",
    "accent_negative":"#DC143C",
    "background_content":"#FFFFFF",
    "background_page":"#F8F9FA",
    "body_text":"#2C3E50",
    "border":"#E1E8ED",
    "border_style":{
        "name":"underlined",
        "borderWidth":"0px 0px 0px 0px",
        "borderStyle":"solid",
        "borderRadius":0
    },
    "button_border":{
        "width":"0px 4px 2px 0px",
        "color":"#D4AF37",
        "radius":"4px"
    },
    "button_capitalization":"none",
    "button_text":"#FFFFFF",
    "button_background_color":"#1B365D",
    "control_border":{
        "width":"1px 1px 1px 1px",
        "color":"#BDC3C7",
        "radius":"4px"
    },
    "control_background_color":"#FFFFFF",
    "control_text":"#2C3E50",
    "card_margin":"20px",
    "card_padding":"8px",
    "card_border":{
        "width":"0px",
        "style":"solid",
        "color":"#E1E8ED",
        "radius":"4px"
    },
    "card_background_color":"#FFFFFF",
    "card_box_shadow":"4px 4px 0px rgba(27,54,93,0.1)",
    "card_outline":{
        "width":"1px",
        "style":"solid",
        "color":"#E1E8ED"
    },
    "card_header_accent":"#D4AF37",
    "card_header_margin":"0px 0px 16px 0px",
    "card_header_padding":"12px",
    "card_header_border":{
        "width":"0px 0px 0px 0px",
        "style":"solid",
        "color":"#E1E8ED",
        "radius":"0px"
    },
    "card_header_background_color":"#FFFFFF",
    "card_title_text":"#1B365D",
    "card_title_font_size":"20px",
    "card_description_background_color":"#FFFFFF",
    "card_description_text":"#5D6D7E",
    "card_description_font_size":"16px",
    "card_menu_background_color":"#FFFFFF",
    "card_menu_text":"#2C3E50",
    "card_header_box_shadow":"0px 0px 0px rgba(0,0,0,0)",
    "card_accent":"#D4AF37",
    "breakpoint_font":"700px",
    "breakpoint_stack_blocks":"700px",
    "colorway":[
        "#1B365D",
        "#D4AF37",
        "#2E8B57",
        "#4682B4",
        "#B8860B",
        "#20B2AA",
        "#CD853F",
        "#708090",
        "#DAA520"
    ],
    "colorscale":[
        "#F8F9FA",
        "#E8F4F8",
        "#D1E7DD",
        "#B3D9E8",
        "#8BB8D8",
        "#6497C8",
        "#4682B4",
        "#2E5984",
        "#1B365D",
        "#0F1B2E"
    ],
    "dbc_primary":"#1B365D",
    "dbc_secondary":"#5D6D7E",
    "dbc_info":"#4682B4",
    "dbc_gray":"#ADB5BD",
    "dbc_success":"#2E8B57",
    "dbc_warning":"#D4AF37",
    "dbc_danger":"#DC143C",
    "font_family":"Roboto",
    "font_family_header":"Roboto",
    "font_family_headings":"Roboto",
    "font_size":"16px",
    "font_size_smaller_screen":"16px",
    "font_size_header":"24px",
    "footer_background_color":"#1B365D",
    "footer_title_text":"#FFFFFF",
    "footer_title_font_size":"24px",
    "title_capitalization":"none",
    "header_content_alignment":"spread",
    "header_margin":"0px 0px 32px 0px",
    "header_padding":"0px 16px 0px 0px",
    "header_border":{
        "width":"1px 1px 1px 1px",
        "style":"solid",
        "color":"#E1E8ED",
        "radius":"0px"
    },
    "header_background_color":"#FFFFFF",
    "header_box_shadow":"4px 4px 0px rgba(0,0,0,0)",
    "header_text":"#1B365D",
    "heading_text":"#1B365D",
    "hero_background_color":"#1B365D",
    "hero_title_text":"#FFFFFF",
    "hero_title_font_size":"48px",
    "hero_subtitle_text":"#D4AF37",
    "hero_subtitle_font_size":"16px",
    "hero_controls_background_color":"rgba(255,255,255,0.9)",
    "hero_controls_label_text":"#2C3E50",
    "hero_controls_label_font_size":"14px",
    "hero_controls_grid_columns":4,
    "hero_controls_accent":"#D4AF37",
    "hero_border":{
        "width":"0",
        "style":"solid",
        "color":"transparent"
    },
    "hero_padding":"24px",
    "hero_gap":"24px",
    "text":"#2C3E50",
    "report_background":"#FFFFFF",
    "report_background_content":"#FFFFFF",
    "report_background_page":"white",
    "report_text":"black",
    "report_font_family":"Roboto",
    "report_font_size":"12px",
    "section_padding":"24px",
    "section_title_font_size":"24px",
    "section_gap":"24px",
    "report_border":"white",
    "graph_grid_color":"#E8F4F8",
    "table_striped_even":"#F8F9FA",
    "table_striped_odd":"#FFFFFF",
    "table_border":"#E1E8ED",
    "tag_background_color":"#E8F4F8",
    "tag_text":"#1B365D",
    "tag_font_size":"14px",
    "tag_border":{
        "width":"0px",
        "style":"solid",
        "color":"#E1E8ED",
        "radius":"4px"
    },
    "tooltip_background_color":"#1B365D",
    "tooltip_text":"#FFFFFF",
    "tooltip_font_size":"14px",
    "top_control_panel_border":{
        "width":"1px",
        "style":"solid",
        "color":"#E1E8ED"
    },
    "color_scheme":"light_with_dark_hero",
    "header_controls_background_color":"#FFFFFF"
}
import os
import sys
from datetime import datetime, time
from typing import TypedDict, Any

from dash import callback, html, dcc, Output, Input
import dash_design_kit as ddk
import numpy as np
import pandas as pd

from data import get_data, cache
from logger import logger

class FILTER_COMPONENT_IDS:
    '''
    A map of all component IDs used in the filter.
    These should all be column names of columns that will be filtered.
    '''
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

class TestInput(TypedDict):
    options: list[Any]
    default: Any

class ComponentResponse(TypedDict):
    layout: html.Div
    test_inputs: dict[str, TestInput]


def component() -> ComponentResponse:
    df = get_data()

    logger.debug("Filter component data loaded. Shape: %s", df.shape)
    logger.debug("Filter component sample data:\n%s", df.head())

    unique_ports = df["ADUANA"].dropna().replace('', np.nan).dropna().unique().tolist()
    unique_ports.sort()

    total_min = df["total"].min()
    total_max = df["total"].max()

    net_weight_min = df["kilo_neto"].min()
    net_weight_max = df["kilo_neto"].max()

    gross_weight_min = df["kilo_bruto"].min()
    gross_weight_max = df["kilo_bruto"].max()

    merchandise_min = df["mercaderias_distintas"].min()
    merchandise_max = df["mercaderias_distintas"].max()

    def classify_port_type(port_name):
        port_name_lower = port_name.lower()
        if "aerop" in port_name_lower or "airport" in port_name_lower:
            return "Airport"
        elif "pto" in port_name_lower or "puerto" in port_name_lower or "port" in port_name_lower:
            return "Seaport"
        elif "za" in port_name_lower or "zona" in port_name_lower or "frca" in port_name_lower:
            return "Free Zone"
        else:
            return "Land Border"

    port_types = list(set([classify_port_type(port) for port in unique_ports]))
    port_types.sort()

    layout = html.Div([ddk._ControlPanel(
        position="top",
        default_open=True,
        control_groups=[
            {
                "title": "Filters",
                "id": "filter_control_group",
                "description": "",
                "children": [
                    html.Div(
                        children=dcc.Dropdown(
                            id=FILTER_COMPONENT_IDS.port_selection,
                            options=[{"label": "All", "value": "all"}] + [{"label": port, "value": port} for port in unique_ports],
                            multi=True,
                            value=["all"]
                        ),
                        id=FILTER_COMPONENT_IDS.port_selection + "_parent",
                        title="Selección de Puerto",
                        style={"minWidth": "200px"}
                    ),

                    html.Div(
                        children=dcc.Dropdown(
                            id=FILTER_COMPONENT_IDS.top_ports_filter,
                            options=[
                                {"label": "All Ports", "value": "all"},
                                {"label": "Top 5", "value": 5},
                                {"label": "Top 10", "value": 10},
                                {"label": "Top 15", "value": 15},
                                {"label": "Top 20", "value": 20}
                            ],
                            value="all"
                        ),
                        id=FILTER_COMPONENT_IDS.top_ports_filter + "_parent",
                        title="Top Puertos",
                        style={"minWidth": "200px"}
                    ),

                    html.Div(
                        children=dcc.Dropdown(
                            id=FILTER_COMPONENT_IDS.port_type_filter,
                            options=[{"label": "All Types", "value": "all"}] + [{"label": ptype, "value": ptype} for ptype in port_types],
                            multi=True,
                            value=["all"]
                        ),
                        id=FILTER_COMPONENT_IDS.port_type_filter + "_parent",
                        title="Tipo de Puerto",
                        style={"minWidth": "200px"}
                    ),

                    html.Div(
                        children=html.Div([
                            dcc.Input(id=FILTER_COMPONENT_IDS.total_value_min, value=total_min, debounce=True, style={"width": 150}),
                            html.Span(" - ", style={"margin": "0 8px", "alignSelf": "center"}),
                            dcc.Input(id=FILTER_COMPONENT_IDS.total_value_max, value=total_max, debounce=True, style={"width": 150})
                        ], style={
                            "display": "flex",
                            "alignItems": "center",
                            "flexWrap": "wrap"
                        }),
                        title="Rango de Valor Total (Gs)"
                    ),

                    html.Div(
                        children=html.Div([
                            dcc.Input(id=FILTER_COMPONENT_IDS.net_weight_min, value=net_weight_min, debounce=True, style={"width": 150}),
                            html.Span(" - ", style={"margin": "0 8px", "alignSelf": "center"}),
                            dcc.Input(id=FILTER_COMPONENT_IDS.net_weight_max, value=net_weight_max, debounce=True, style={"width": 150})
                        ], style={
                            "display": "flex",
                            "alignItems": "center",
                            "flexWrap": "wrap"
                        }),
                        title="Rango de Peso Neto (kg)"
                    ),

                    html.Div(
                        children=html.Div([
                            dcc.Input(id=FILTER_COMPONENT_IDS.gross_weight_min, value=gross_weight_min, debounce=True, style={"width": 150}),
                            html.Span(" - ", style={"margin": "0 8px", "alignSelf": "center"}),
                            dcc.Input(id=FILTER_COMPONENT_IDS.gross_weight_max, value=gross_weight_max, debounce=True, style={"width": 150})
                        ], style={
                            "display": "flex",
                            "alignItems": "center",
                            "flexWrap": "wrap"
                        }),
                        title="Rango de Peso Bruto (kg)"
                    ),

                    html.Div(
                        children=html.Div([
                            dcc.Input(id=FILTER_COMPONENT_IDS.merchandise_count_min, value=merchandise_min, debounce=True, style={"width": 150}),
                            html.Span(" - ", style={"margin": "0 8px", "alignSelf": "center"}),
                            dcc.Input(id=FILTER_COMPONENT_IDS.merchandise_count_max, value=merchandise_max, debounce=True, style={"width": 150})
                        ], style={
                            "display": "flex",
                            "alignItems": "center",
                            "flexWrap": "wrap"
                        }),
                        title="Rango de Diversidad de Mercaderías"
                    )
                ],
            },
        ],
    ), html.Div(id='total_results', style={ 'paddingTop': 20, 'marginLeft': 50, 'fontStyle': 'italic', 'minHeight': 45 })])

    test_inputs: dict[str, TestInput] = {
        "port_selection": {
            "options": ["all"] + unique_ports[:3],
            "default": ["all"]
        },
        "total_value_min": {
            "options": [total_min, (total_min + total_max) / 2, total_max],
            "default": total_min
        },
        "total_value_max": {
            "options": [total_max, (total_min + total_max) / 2, total_min],
            "default": total_max
        },
        "net_weight_min": {
            "options": [net_weight_min, (net_weight_min + net_weight_max) / 2, net_weight_max],
            "default": net_weight_min
        },
        "net_weight_max": {
            "options": [net_weight_max, (net_weight_min + net_weight_max) / 2, net_weight_min],
            "default": net_weight_max
        },
        "gross_weight_min": {
            "options": [gross_weight_min, (gross_weight_min + gross_weight_max) / 2, gross_weight_max],
            "default": gross_weight_min
        },
        "gross_weight_max": {
            "options": [gross_weight_max, (gross_weight_min + gross_weight_max) / 2, gross_weight_min],
            "default": gross_weight_max
        },
        "merchandise_count_min": {
            "options": [merchandise_min, (merchandise_min + merchandise_max) / 2, merchandise_max],
            "default": merchandise_min
        },
        "merchandise_count_max": {
            "options": [merchandise_max, (merchandise_min + merchandise_max) / 2, merchandise_min],
            "default": merchandise_max
        },
        "top_ports_filter": {
            "options": ["all", 5, 10, 15, 20],
            "default": "all"
        },
        "port_type_filter": {
            "options": ["all"] + port_types,
            "default": ["all"]
        }
    }

    return {
        "layout": layout,
        "test_inputs": test_inputs
    }

@cache.memoize()
def filter_data(df, **filters):
    logger.debug("Starting data filtering. Original shape: %s", df.shape)
    logger.debug("Applied filters: %s", filters)

    df = df.copy()

    def classify_port_type(port_name):
        port_name_lower = port_name.lower()
        if "aerop" in port_name_lower or "airport" in port_name_lower:
            return "Airport"
        elif "pto" in port_name_lower or "puerto" in port_name_lower or "port" in port_name_lower:
            return "Seaport"
        elif "za" in port_name_lower or "zona" in port_name_lower or "frca" in port_name_lower:
            return "Free Zone"
        else:
            return "Land Border"

    if len(filters["port_selection"]) > 0 and "all" not in filters["port_selection"]:
        df = df[df["ADUANA"].isin(filters["port_selection"])]

    if filters["top_ports_filter"] != "all":
        top_n = int(filters["top_ports_filter"])
        top_ports = df.nlargest(top_n, "total")["ADUANA"].tolist()
        df = df[df["ADUANA"].isin(top_ports)]

    if len(filters["port_type_filter"]) > 0 and "all" not in filters["port_type_filter"]:
        df["port_type"] = df["ADUANA"].apply(classify_port_type)
        df = df[df["port_type"].isin(filters["port_type_filter"])]

    if "total" in df.columns:
        df = df[df["total"] >= float(filters["total_value_min"])]
        df = df[df["total"] <= float(filters["total_value_max"])]

    if "kilo_neto" in df.columns:
        df = df[df["kilo_neto"] >= float(filters["net_weight_min"])]
        df = df[df["kilo_neto"] <= float(filters["net_weight_max"])]

    if "kilo_bruto" in df.columns:
        df = df[df["kilo_bruto"] >= float(filters["gross_weight_min"])]
        df = df[df["kilo_bruto"] <= float(filters["gross_weight_max"])]

    if "mercaderias_distintas" in df.columns:
        df = df[df["mercaderias_distintas"] >= int(filters["merchandise_count_min"])]
        df = df[df["mercaderias_distintas"] <= int(filters["merchandise_count_max"])]

    logger.debug("Filtering complete. Final shape: %s", df.shape)
    logger.debug("Filtered data sample:\n%s", df.head())

    return df

@callback(Output("total_results", "children"), inputs=FILTER_CALLBACK_INPUTS)
def display_count(**kwargs):
    df = get_data()
    # Get total count
    count = len(df)

    filtered_df = filter_data(df, **kwargs)
    # Get filtered count
    filtered_count = len(filtered_df)

    return f"{filtered_count:,} / {count:,} rows"


from dash import callback, html, dcc, Output, Input
import dash_design_kit as ddk
import numpy as np
import pandas as pd
import sys
import traceback
import os
from logger import logger

from data import get_data
from components.filter_component import filter_data, FILTER_CALLBACK_INPUTS

def component():
    '''Return a component with data cards displaying key metrics'''
    logger.debug("Creating data cards component for Paraguay import operations")
    
    layout = ddk.Row(
        id="data_cards",
        children=[
            ddk.DataCard(
                id='total_ports_card',
                value='...',
                label='Cantidad de Puertos',
                width=20
            ),
            ddk.DataCard(
                id='total_weight_card',
                value='...',
                label='Peso Total (toneladas)',
                width=20
            ),
            ddk.DataCard(
                id='avg_weight_card',
                value='...',
                label='Peso Promedio por Puerto (toneladas)',
                width=20
            ),
            ddk.DataCard(
                id='top_port_card',
                value='...',
                label='Puerto principal por peso',
                width=20
            ),
            ddk.DataCard(
                id='total_value_card',
                value='...',
                label='Valor Total Importado (Gs)',
                width=20
            )
        ]
    )

    return {
        "layout": layout,
        "test_inputs": {}
    }

@callback(
    [
        Output('total_ports_card', 'value'),
        Output('total_weight_card', 'value'),
        Output('avg_weight_card', 'value'),
        Output('top_port_card', 'value'),
        Output('total_value_card', 'value')
    ],
    FILTER_CALLBACK_INPUTS
)
def update(**kwargs):
    '''Update all data cards with the filtered data metrics'''
    try:
        logger.debug("Starting data cards update with kwargs: %s", kwargs)
        
        # Get data and apply filters
        df = filter_data(get_data(), **kwargs)
        
        logger.debug("Filtered data shape: %s", df.shape)
        
        # Check if dataframe is empty
        if len(df) == 0:
            logger.debug("No data after filtering")
            return ["No Data", "No Data", "No Data", "No Data", "No Data"]

        # Calculate key metrics for Paraguay import operations
        total_ports = len(df)
        
        # Total weight in tons (kilo_neto es en kg → convertir a toneladas)
        total_weight_tons = df["kilo_neto"].sum() / 1000
        
        # Average weight per port in tons
        avg_weight_tons = total_weight_tons / total_ports if total_ports > 0 else 0
        
        # Find top port by weight
        top_port = df.loc[df["kilo_neto"].idxmax(), "ADUANA"]
        
        # Total value
        total_value = df["total"].sum()

        # Formato valores
        total_ports_formatted = f"{total_ports:,}"
        total_weight_formatted = f"{total_weight_tons:,.0f}"
        avg_weight_formatted = f"{avg_weight_tons:,.0f}"
        top_port_formatted = str(top_port)[:20] + "..." if len(str(top_port)) > 20 else str(top_port)
        total_value_formatted = f"Gs {total_value:,.0f}"

        logger.debug("Métricas calculadas - Puertos: %s, Peso total: %s toneladas, Puerto principal: %s", 
                    total_ports, total_weight_tons, top_port)


        return [
            total_ports_formatted,
            total_weight_formatted, 
            avg_weight_formatted,
            top_port_formatted,
            total_value_formatted
        ]
        
    except Exception as e:
        logger.debug("Error updating data cards: %s", str(e))
        print(f"Error updating data cards: {e}\n{traceback.format_exc()}")
        return ["Error", "Error", "Error", "Error", "Error"]

from dash import callback, html, dcc, Output, Input
import dash_design_kit as ddk
import dash_ag_grid as dag
import pandas as pd
import numpy as np
import sys
import traceback
import os

from data import get_data
from components.filter_component import filter_data, FILTER_CALLBACK_INPUTS
from logger import logger


def component():
    '''Return a component with an Ag-Grid table displaying filtered data'''
    layout = ddk.Card(
        id="data_table",
        children=[
            ddk.CardHeader(title="Paraguay Import Operations - Port Rankings"),
            html.Div(id="data_table_title", style={"padding": "10px", "fontWeight": "bold"}),
            dag.AgGrid(
                id="data_table_grid",
                columnDefs=[],
                dashGridOptions={
                    "pagination": True,
                    "paginationPageSize": 50,
                    "domLayout": "normal",
                    "rowSelection": "multiple",
                    "defaultColDef": {
                        "sortable": True,
                        "filter": True,
                        "resizable": True,
                        "floatingFilter": True
                    }
                },
                rowData=[]
            ),
            ddk.CardFooter(title="Port rankings by total value, weight, and merchandise diversity. Limited to a maximum of 10000 rows.")
        ],
        width=100
    )

    return {
        "layout": layout,
        "test_inputs": {}
    }

def _update_logic(**kwargs):
    '''Core data table update logic without error handling.'''
    logger.debug("Starting data table creation")
    
    df = filter_data(get_data(), **kwargs)
    
    logger.debug("Data table df shape: %s", df.shape)
    
    if len(df) == 0:
        return [], []

    df = df.head(10_000)
    
    # Create ranking metrics for ports
    port_rankings = df.groupby('ADUANA').agg({
        'total': 'sum',
        'kilo_neto': 'sum', 
        'kilo_bruto': 'sum',
        'mercaderias_distintas': 'sum'
    }).reset_index()
    
    # Add ranking columns
    port_rankings['total_rank'] = port_rankings['total'].rank(method='dense', ascending=False).astype(int)
    port_rankings['weight_rank'] = port_rankings['kilo_neto'].rank(method='dense', ascending=False).astype(int)
    port_rankings['diversity_rank'] = port_rankings['mercaderias_distintas'].rank(method='dense', ascending=False).astype(int)
    
    # Sort by total value (most important metric)
    port_rankings = port_rankings.sort_values('total', ascending=False)
    
    logger.debug("Port rankings created with %d ports", len(port_rankings))

    column_defs = [
        {
            "headerName": "Ranking (por Valor)",
            "field": "total_rank",
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "width": 150,
            "pinned": "left"
        },
        {
            "headerName": "Aduana / Puerto",
            "field": "ADUANA",
            "filter": "agTextColumnFilter",
            "width": 200,
            "pinned": "left"
        },
        {
            "headerName": "Valor Total (Gs)",
            "field": "total",
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
            "width": 150
        },
        {
            "headerName": "Peso Neto (kg)",
            "field": "kilo_neto", 
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
            "width": 150
        },
        {
            "headerName": "Peso Bruto (kg)",
            "field": "kilo_bruto",
            "type": "numericColumn", 
            "filter": "agNumberColumnFilter",
            "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
            "width": 150
        },
        {
            "headerName": "Diversidad de Mercaderías",
            "field": "mercaderias_distintas",
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
            "width": 150
        },
        {
            "headerName": "Ranking por Peso",
            "field": "weight_rank",
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "width": 120
        },
        {
            "headerName": "Ranking por Diversidad",
            "field": "diversity_rank",
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "width": 130
        }
    ]

    df_cleaned = port_rankings.copy()
    
    for col in df_cleaned.columns:
        if pd.api.types.is_numeric_dtype(df_cleaned[col]):
            df_cleaned[col] = df_cleaned[col].replace('', np.nan)
    
    row_data = df_cleaned.to_dict('records')

    for row in row_data:
        for key, value in row.items():
            if pd.isna(value):
                row[key] = None
    
    return column_defs, row_data

@callback(
    output=[
        Output("data_table_grid", "columnDefs"),
        Output("data_table_grid", "rowData"),
        Output("data_table_title", "children")
    ],
    inputs=FILTER_CALLBACK_INPUTS
)
def update(**kwargs):
    '''Update the data table based on filters and controls'''
    try:
        column_defs, row_data = _update_logic(**kwargs)
        title_text = f"Showing {len(row_data)} ports ranked by import operations"
        return column_defs, row_data, title_text

    except Exception as e:
        logger.debug("Error updating data table: %s", str(e))
        print(f"Error updating data table: {e}, {traceback.format_exc()}")
        return [], [], "Error loading data"
    

import sys
import os
import traceback
from typing import TypedDict, Any, Tuple
from datetime import datetime, timedelta

from dash import callback, html, dcc, Output, Input
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from data import get_data
from components.filter_component import filter_data, FILTER_CALLBACK_INPUTS
from logger import logger

class TestInput(TypedDict):
    options: list[Any]
    default: Any

class ComponentResponse(TypedDict):
    layout: ddk.Card
    test_inputs: dict[str, TestInput]

component_id = "port_ranking_value"

def component() -> ComponentResponse:
    graph_id = f"{component_id}_graph"
    error_id = f"{component_id}_error"
    loading_id = f"{component_id}_loading"

    top_n_id = f"{component_id}_top_n"
    top_n_options = [
        {"label": "Top 5", "value": 5},
        {"label": "Top 10", "value": 10},
        {"label": "Top 15", "value": 15},
        {"label": "Top 20", "value": 20}
    ]
    top_n_default = 10

    sort_order_id = f"{component_id}_sort_order"
    sort_order_options = [
        {"label": "Descendente (Mayor a Menor)", "value": "desc"},
        {"label": "Ascendente (Menor a Mayor)", "value": "asc"}
    ]
    sort_order_default = "desc"

    title = "Ranking de Puertos por Valor Total de Importación"
    description = "Ranking de aduanas y puertos según el valor total de importación, mostrando los más relevantes para las operaciones de Paraguay."

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap", "rowGap": "10px", "alignItems": "center", "marginBottom": "15px"},
                children=[
                    html.Div(
                        children=[
                            html.Label("Cantidad de Puertos:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            dcc.Dropdown(
                                id=top_n_id,
                                options=top_n_options,
                                value=top_n_default,
                                style={"minWidth": "200px"}
                            )
                        ],
                        style={"display": "flex", "flexDirection": "column", "marginRight": "15px"}
                    ),
                    html.Div(
                        children=[
                            html.Label("Orden:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            dcc.Dropdown(
                                id=sort_order_id,
                                options=sort_order_options,
                                value=sort_order_default,
                                style={"minWidth": "200px"}
                            )
                        ],
                        style={"display": "flex", "flexDirection": "column", "marginRight": "15px"}
                    ),
                ],
            ),
            dcc.Loading(
                id=loading_id,
                type="circle",
                children=[
                    ddk.Graph(id=graph_id),
                ]
            ),
            html.Pre(id=error_id, style={"color": "red", "margin": "10px 0"}),
            ddk.CardFooter(title=description)
        ],
        width=50
    )

    test_inputs: dict[str, TestInput] = {
        top_n_id: {
            "options": [5, 10, 15, 20],
            "default": top_n_default
        },
        sort_order_id: {
            "options": ["desc", "asc"],
            "default": sort_order_default
        }
    }

    return {
        "layout": layout,
        "test_inputs": test_inputs
    }


def _update_logic(**kwargs) -> go.Figure:
    """Lógica principal del gráfico de ranking de puertos por valor de importación (sin manejo de errores)."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Sin datos disponibles",
            annotations=[{
                "text": "No hay datos para mostrar",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    top_n = kwargs.get(f'{component_id}_top_n', 10)
    sort_order = kwargs.get(f'{component_id}_sort_order', "desc")
    
    if top_n is None:
        top_n = 10
    if sort_order is None:
        sort_order = "desc"

    logger.debug("Creando gráfico. df:\n%s", df.head())
    logger.debug("Top N: %s, Orden: %s", top_n, sort_order)

    port_totals = df.groupby('ADUANA')['total'].sum().reset_index()
    
    ascending = sort_order == "asc"
    port_totals = port_totals.sort_values('total', ascending=ascending)
    
    port_totals = port_totals.head(top_n)
    
    if sort_order == "desc":
        port_totals = port_totals.sort_values('total', ascending=True)
    else:
        port_totals = port_totals.sort_values('total', ascending=False)

    # Convertir a billones de guaraníes
    port_totals['total_billones'] = port_totals['total'] / 1e12

    logger.debug("Totales por puerto después del procesamiento:\n%s", port_totals.head())

    fig = px.bar(
        port_totals,
        x='total_billones',
        y='ADUANA',
        orientation='h',
        labels={
            'total_billones': 'Valor Total de Importación (Billones de Gs)',
            'ADUANA': 'Aduana / Puerto'
        }
    )

    fig.update_traces(
        hovertemplate="<b>%{y}</b><br>Valor Total: %{x:.2f} billones Gs<extra></extra>"
    )

    fig.update_layout(
        xaxis_title="Valor Total de Importación (Billones de Gs)",
        yaxis_title="Aduana / Puerto",
        height=max(400, len(port_totals) * 30 + 100)
    )

    return fig


@callback(
    output=[
        Output(f"{component_id}_graph", "figure"),
        Output(f"{component_id}_error", "children")
    ],
    inputs={
        f'{component_id}_top_n': Input(f"{component_id}_top_n", "value"),
        f'{component_id}_sort_order': Input(f"{component_id}_sort_order", "value"),
        **FILTER_CALLBACK_INPUTS
    }
)
def update(**kwargs) -> Tuple[go.Figure, str]:
    empty_fig = go.Figure()
    empty_fig.update_layout(
        title="Error en el gráfico",
        annotations=[{"text": "Ocurrió un error al actualizar el gráfico", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error actualizando el gráfico: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return empty_fig, error_msg

    
import sys
import os
import traceback
from typing import TypedDict, Any, Tuple
from datetime import datetime, timedelta

from dash import callback, html, dcc, Output, Input
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from data import get_data
from components.filter_component import filter_data, FILTER_CALLBACK_INPUTS
from logger import logger

class TestInput(TypedDict):
    options: list[Any]
    default: Any

class ComponentResponse(TypedDict):
    layout: ddk.Card
    test_inputs: dict[str, TestInput]

component_id = "volume_vs_value_scatter"

def component() -> ComponentResponse:
    graph_id = f"{component_id}_graph"
    error_id = f"{component_id}_error"
    loading_id = f"{component_id}_loading"

    # Selector de tipo de peso
    weight_type_id = f"{component_id}_weight_type"
    weight_type_options = [
        {"label": "Peso Neto (kg)", "value": "kilo_neto"},
        {"label": "Peso Bruto (kg)", "value": "kilo_bruto"}
    ]
    weight_type_default = "kilo_neto"

    # Control del factor de tamaño de puntos
    size_factor_id = f"{component_id}_size_factor"
    size_factor_min = 0.1
    size_factor_max = 2.0
    size_factor_default = 1.0
    size_factor_marks = {
        0.1: "0.1x",
        0.5: "0.5x",
        1.0: "1.0x",
        1.5: "1.5x",
        2.0: "2.0x"
    }

    title = "Volumen vs Valor Monetario de Importación"
    description = "Gráfico de dispersión que muestra la relación entre el volumen de importación (peso) y el valor monetario, con el tamaño de los puntos representando la diversidad de mercaderías."

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={
                    "display": "flex", 
                    "flexDirection": "row", 
                    "flexWrap": "wrap", 
                    "rowGap": "10px", 
                    "alignItems": "center", 
                    "marginBottom": "15px"
                },
                children=[
                    html.Div(
                        children=[
                            html.Label("Tipo de Peso:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            dcc.Dropdown(
                                id=weight_type_id,
                                options=weight_type_options,
                                value=weight_type_default,
                                style={"minWidth": "200px"}
                            )
                        ],
                        style={"display": "flex", "flexDirection": "column", "marginRight": "15px"}
                    ),
                    html.Div(
                        children=[
                            html.Label("Factor de Tamaño de Puntos:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            html.Div(
                                children=dcc.Slider(
                                    id=size_factor_id,
                                    min=size_factor_min,
                                    max=size_factor_max,
                                    step=0.1,
                                    value=size_factor_default,
                                    marks=size_factor_marks,
                                    tooltip={"placement": "bottom", "always_visible": True}
                                ),
                                style={"minWidth": "200px"}
                            )
                        ],
                        style={
                            "display": "flex", 
                            "flexDirection": "column", 
                            "marginRight": "15px", 
                            "width": "300px"
                        }
                    ),
                ],
            ),
            dcc.Loading(
                id=loading_id,
                type="circle",
                children=[
                    ddk.Graph(id=graph_id),
                ]
            ),
            html.Pre(id=error_id, style={"color": "red", "margin": "10px 0"}),
            ddk.CardFooter(title=description)
        ],
        width=50
    )

    test_inputs: dict[str, TestInput] = {
        weight_type_id: {
            "options": [option["value"] for option in weight_type_options],
            "default": weight_type_default
        },
        size_factor_id: {
            "options": [0.1, 0.5, 1.0, 1.5, 2.0],
            "default": size_factor_default
        }
    }

    return {
        "layout": layout,
        "test_inputs": test_inputs
    }


def _update_logic(**kwargs) -> go.Figure:
    """Lógica principal del gráfico de ranking de puertos por valor de importación (sin manejo de errores)."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Sin datos disponibles",
            annotations=[{
                "text": "No hay datos para mostrar",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    top_n = kwargs.get(f'{component_id}_top_n', 10)
    sort_order = kwargs.get(f'{component_id}_sort_order', "desc")
    
    if top_n is None:
        top_n = 10
    if sort_order is None:
        sort_order = "desc"

    logger.debug("Creando gráfico. df:\n%s", df.head())
    logger.debug("Top N: %s, Orden: %s", top_n, sort_order)

    port_totals = df.groupby('ADUANA')['total'].sum().reset_index()
    
    ascending = sort_order == "asc"
    port_totals = port_totals.sort_values('total', ascending=ascending)
    
    port_totals = port_totals.head(top_n)
    
    if sort_order == "desc":
        port_totals = port_totals.sort_values('total', ascending=True)
    else:
        port_totals = port_totals.sort_values('total', ascending=False)

    # Convertir a billones de guaraníes
    port_totals['total_billones'] = port_totals['total'] / 1e12

    logger.debug("Totales por puerto después del procesamiento:\n%s", port_totals.head())

    fig = px.bar(
        port_totals,
        x='total_billones',
        y='ADUANA',
        orientation='h',
        labels={
            'total_billones': 'Valor Total de Importación (Billones de Gs)',
            'ADUANA': 'Aduana / Puerto'
        }
    )

    fig.update_traces(
        hovertemplate="<b>%{y}</b><br>Valor Total: %{x:.2f} billones Gs<extra></extra>"
    )

    fig.update_layout(
        xaxis_title="Valor Total de Importación (Billones de Gs)",
        yaxis_title="Aduana / Puerto",
        height=max(400, len(port_totals) * 30 + 100)
    )

    return fig


@callback(
    output=[
        Output(f"{component_id}_graph", "figure"),
        Output(f"{component_id}_error", "children")
    ],
    inputs={
        f'{component_id}_top_n': Input(f"{component_id}_top_n", "value"),
        f'{component_id}_sort_order': Input(f"{component_id}_sort_order", "value"),
        **FILTER_CALLBACK_INPUTS
    }
)
def update(**kwargs) -> Tuple[go.Figure, str]:
    empty_fig = go.Figure()
    empty_fig.update_layout(
        title="Error en el gráfico",
        annotations=[{"text": "Ocurrió un error al actualizar el gráfico", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error actualizando el gráfico: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return empty_fig, error_msg
    
import sys
import os
import traceback
from typing import TypedDict, Any, Tuple
from datetime import datetime, timedelta

from dash import callback, html, dcc, Output, Input
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from data import get_data
from components.filter_component import filter_data, FILTER_CALLBACK_INPUTS
from logger import logger

class TestInput(TypedDict):
    options: list[Any]
    default: Any

class ComponentResponse(TypedDict):
    layout: ddk.Card
    test_inputs: dict[str, TestInput]

component_id = "merchandise_diversity_ranking"

def component() -> ComponentResponse:
    graph_id = f"{component_id}_graph"
    error_id = f"{component_id}_error"
    loading_id = f"{component_id}_loading"

    sort_metric_id = f"{component_id}_sort_metric"
    sort_metric_options = [
        {"label": "Cantidad de Mercaderías", "value": "mercaderias_distintas"},
        {"label": "Valor Total (Gs)", "value": "total"},
        {"label": "Peso Neto (kg)", "value": "kilo_neto"}
    ]
    sort_metric_default = "mercaderias_distintas"

    min_merchandise_id = f"{component_id}_min_merchandise"
    max_merchandise_id = f"{component_id}_max_merchandise"

    df = get_data()
    min_merchandise = int(df["mercaderias_distintas"].min())
    max_merchandise = int(df["mercaderias_distintas"].max())

    title = "Ranking de Puertos por Diversidad de Mercaderías"
    description = "Ranking de aduanas y puertos según la diversidad de mercaderías, el valor total de importación o el peso neto, con opciones de filtrado."

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap", "rowGap": "10px", "alignItems": "center", "marginBottom": "15px"},
                children=[
                    html.Div(
                        children=[
                            html.Label("Ordenar por:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            dcc.Dropdown(
                                id=sort_metric_id,
                                options=sort_metric_options,
                                value=sort_metric_default,
                                style={"minWidth": "200px"}
                            )
                        ],
                        style={"display": "flex", "flexDirection": "column", "marginRight": "15px"}
                    ),
                    html.Div(
                        children=[
                            html.Label("Rango de Cantidad de Mercaderías:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            html.Div([
                                dcc.Input(
                                    id=min_merchandise_id,
                                    type="number",
                                    value=min_merchandise,
                                    min=min_merchandise,
                                    max=max_merchandise,
                                    placeholder="Mínimo",
                                    debounce=True,
                                    style={"width": 100}
                                ),
                                html.Span(" - ", style={"margin": "0 8px", "alignSelf": "center"}),
                                dcc.Input(
                                    id=max_merchandise_id,
                                    type="number",
                                    value=max_merchandise,
                                    min=min_merchandise,
                                    max=max_merchandise,
                                    placeholder="Máximo",
                                    debounce=True,
                                    style={"width": 100}
                                )
                            ], style={
                                "display": "flex",
                                "alignItems": "center",
                                "flexWrap": "wrap"
                            })
                        ],
                        style={"display": "flex", "flexDirection": "column", "marginRight": "15px"}
                    )
                ],
            ),
            dcc.Loading(
                id=loading_id,
                type="circle",
                children=[
                    ddk.Graph(id=graph_id),
                ]
            ),
            html.Pre(id=error_id, style={"color": "red", "margin": "10px 0"}),
            ddk.CardFooter(title=description)
        ],
        width=50
    )

    test_inputs: dict[str, TestInput] = {
        sort_metric_id: {
            "options": [option["value"] for option in sort_metric_options],
            "default": sort_metric_default
        },
        min_merchandise_id: {
            "options": [min_merchandise, min_merchandise + 1000, min_merchandise + 5000],
            "default": min_merchandise
        },
        max_merchandise_id: {
            "options": [max_merchandise - 5000, max_merchandise - 1000, max_merchandise],
            "default": max_merchandise
        }
    }

    return {
        "layout": layout,
        "test_inputs": test_inputs
    }


def _update_logic(**kwargs) -> go.Figure:
    """Lógica principal del gráfico de ranking de diversidad de mercaderías (sin manejo de errores)."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Sin datos disponibles",
            annotations=[{
                "text": "No hay datos para mostrar",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    sort_metric = kwargs.get(f'{component_id}_sort_metric', 'mercaderias_distintas')
    min_merchandise = kwargs.get(f'{component_id}_min_merchandise')
    max_merchandise = kwargs.get(f'{component_id}_max_merchandise')

    if sort_metric is None:
        sort_metric = 'mercaderias_distintas'

    df['mercaderias_distintas'] = pd.to_numeric(df['mercaderias_distintas'], errors='coerce')
    
    if min_merchandise is not None:
        min_merchandise = float(min_merchandise)
        df = df[df['mercaderias_distintas'] >= min_merchandise]
    
    if max_merchandise is not None:
        max_merchandise = float(max_merchandise)
        df = df[df['mercaderias_distintas'] <= max_merchandise]

    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Sin datos después del filtrado",
            annotations=[{
                "text": "Ningún dato coincide con los criterios de filtro",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    df_sorted = df.sort_values(by=sort_metric, ascending=False)

    logger.debug("Creando gráfico. df:\n%s", df_sorted.head())

    metric_labels = {
        'mercaderias_distintas': 'Cantidad de Mercaderías',
        'total': 'Valor Total (Gs)',
        'kilo_neto': 'Peso Neto (kg)'
    }

    y_title = metric_labels.get(sort_metric, sort_metric)

    fig = px.bar(
        df_sorted,
        x='ADUANA',
        y=sort_metric,
        title="",
        labels={
            'ADUANA': 'Aduana / Puerto',
            sort_metric: y_title
        }
    )

    fig.update_layout(
        xaxis_title="Aduana / Puerto",
        yaxis_title=y_title,
        xaxis={'categoryorder': 'total descending'}
    )

    fig.update_xaxes(tickangle=45)

    # Hover dinámico según métrica
    fig.update_traces(
        hovertemplate="<b>%{x}</b><br>" + f"{y_title}: %{{y:,.0f}}<extra></extra>"
    )

    return fig


@callback(
    output=[
        Output(f"{component_id}_graph", "figure"),
        Output(f"{component_id}_error", "children")
    ],
    inputs={
        f'{component_id}_sort_metric': Input(f"{component_id}_sort_metric", "value"),
        f'{component_id}_min_merchandise': Input(f"{component_id}_min_merchandise", "value"),
        f'{component_id}_max_merchandise': Input(f"{component_id}_max_merchandise", "value"),
        **FILTER_CALLBACK_INPUTS
    }
)
def update(**kwargs) -> Tuple[go.Figure, str]:
    empty_fig = go.Figure()
    empty_fig.update_layout(
        title="Error en el gráfico",
        annotations=[{"text": "Ocurrió un error al actualizar el gráfico", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error actualizando el gráfico: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return empty_fig, error_msg

    
import sys
import os
import traceback
from typing import TypedDict, Any, Tuple
from datetime import datetime, timedelta

from dash import callback, html, dcc, Output, Input
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from data import get_data
from components.filter_component import filter_data, FILTER_CALLBACK_INPUTS
from logger import logger

class TestInput(TypedDict):
    options: list[Any]
    default: Any

class ComponentResponse(TypedDict):
    layout: ddk.Card
    test_inputs: dict[str, TestInput]

component_id = "weight_comparison_treemap"

# Definir valor por defecto a nivel de módulo
weight_type_default = "kilo_neto"

def component() -> ComponentResponse:
    graph_id = f"{component_id}_graph"
    error_id = f"{component_id}_error"
    loading_id = f"{component_id}_loading"

    weight_type_id = f"{component_id}_weight_type"
    weight_type_options = [
        {"label": "Peso Neto (kg)", "value": "kilo_neto"},
        {"label": "Peso Bruto (kg)", "value": "kilo_bruto"}
    ]

    title = "Distribución de Peso por Puerto"
    description = "Treemap que muestra la distribución del peso de importaciones en las aduanas, con el color indicando el valor total importado."

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap", "rowGap": "10px", "alignItems": "center", "marginBottom": "15px"},
                children=[
                    html.Div(
                        children=[
                            html.Label("Tipo de Peso:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            dcc.Dropdown(
                                id=weight_type_id,
                                options=weight_type_options,
                                value=weight_type_default,
                                style={"minWidth": "200px"}
                            )
                        ],
                        style={"display": "flex", "flexDirection": "column", "marginRight": "15px"}
                    ),
                ],
            ),
            dcc.Loading(
                id=loading_id,
                type="circle",
                children=[
                    ddk.Graph(id=graph_id),
                ]
            ),
            html.Pre(id=error_id, style={"color": "red", "margin": "10px 0"}),
            ddk.CardFooter(title=description)
        ],
        width=50
    )

    test_inputs: dict[str, TestInput] = {
        weight_type_id: {
            "options": [option["value"] for option in weight_type_options],
            "default": weight_type_default
        }
    }

    return {
        "layout": layout,
        "test_inputs": test_inputs
    }


def _update_logic(**kwargs) -> go.Figure:
    """Lógica principal del treemap de distribución de peso (sin manejo de errores)."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            annotations=[{
                "text": "Sin datos disponibles",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    weight_type = kwargs.get(f'{component_id}_weight_type', weight_type_default)
    if weight_type is None:
        weight_type = weight_type_default

    # Agregar datos por aduana
    df_agg = df.groupby('ADUANA').agg({
        'kilo_neto': 'sum',
        'kilo_bruto': 'sum',
        'total': 'sum',
        'mercaderias_distintas': 'sum'
    }).reset_index()

    # Peso en millones de kilogramos
    df_agg['peso_millones'] = df_agg[weight_type] / 1_000_000
    
    # Valor en billones de guaraníes
    df_agg['valor_billones'] = df_agg['total'] / 1_000_000_000_000

    # Filtrar aduanas muy pequeñas para mantener legibilidad
    min_weight_threshold = df_agg['peso_millones'].quantile(0.05)
    df_filtered = df_agg[df_agg['peso_millones'] >= min_weight_threshold].copy()

    if len(df_filtered) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            annotations=[{
                "text": "No hay datos significativos para mostrar",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    # Ordenar por peso
    df_filtered = df_filtered.sort_values('peso_millones', ascending=False)

    logger.debug("Creando treemap. df_filtered:\n%s", df_filtered.head())

    # Etiqueta de peso según tipo
    peso_label = "Peso Neto" if weight_type == "kilo_neto" else "Peso Bruto"
    
    fig = px.treemap(
        df_filtered,
        path=['ADUANA'],
        values='peso_millones',
        color='valor_billones',
        color_continuous_scale='Viridis',
        hover_data={
            'peso_millones': ':.1f',
            'valor_billones': ':.2f',
            'mercaderias_distintas': ':,'
        }
    )

    # Hover en español
    fig.update_traces(
        hovertemplate="<b>%{label}</b><br>" +
                     f"{peso_label}: %{{value:.1f}} M kg<br>" +
                     "Valor Importado: %{color:.2f} billones Gs<br>" +
                     "Cantidad de Mercaderías: %{customdata[2]:,}<extra></extra>",
        textinfo="label+value"
    )

    # Layout
    fig.update_layout(
        coloraxis_colorbar=dict(
            title="Valor Importado<br>(Billones de Gs)",
            title_side="right"
        ),
        margin=dict(t=10, l=10, r=10, b=10)
    )

    return fig


@callback(
    output=[
        Output(f"{component_id}_graph", "figure"),
        Output(f"{component_id}_error", "children")
    ],
    inputs={
        f'{component_id}_weight_type': Input(f"{component_id}_weight_type", "value"),
        **FILTER_CALLBACK_INPUTS
    }
)
def update(**kwargs) -> Tuple[go.Figure, str]:
    empty_fig = go.Figure()
    empty_fig.update_layout(
        annotations=[{"text": "Ocurrió un error al actualizar este gráfico", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error actualizando el gráfico: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return empty_fig, error_msg

import sys
import os
import traceback
from typing import TypedDict, Any, Tuple
from datetime import datetime, timedelta

from dash import callback, html, dcc, Output, Input
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from data import get_data
from components.filter_component import filter_data, FILTER_CALLBACK_INPUTS
from logger import logger

class TestInput(TypedDict):
    options: list[Any]
    default: Any

class ComponentResponse(TypedDict):
    layout: ddk.Card
    test_inputs: dict[str, TestInput]

component_id = "port_performance_radar"

def component() -> ComponentResponse:
    graph_id = f"{component_id}_graph"
    error_id = f"{component_id}_error"
    loading_id = f"{component_id}_loading"

    # Obtener los puertos principales
    df = get_data()
    top_ports = df.groupby('ADUANA')['total'].sum().nlargest(10).index.tolist()
    
    port_selector_id = f"{component_id}_ports"
    port_options = [{"label": port, "value": port} for port in top_ports]
    port_default = top_ports[:3] if len(top_ports) >= 3 else top_ports
    
    normalize_id = f"{component_id}_normalize"

    title = "Radar de Desempeño de Puertos"
    description = "Comparación multi-métrica de los principales puertos de Paraguay mostrando valor total, métricas de peso y diversidad de mercaderías."

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap", "rowGap": "10px", "alignItems": "center", "marginBottom": "15px"},
                children=[
                    html.Div(
                        children=[
                            html.Label("Seleccionar Puertos:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            dcc.Dropdown(
                                id=port_selector_id,
                                options=port_options,
                                value=port_default,
                                multi=True,
                                style={"minWidth": "300px"}
                            )
                        ],
                        style={"display": "flex", "flexDirection": "column", "marginRight": "15px"}
                    ),
                    html.Div(
                        children=[
                            html.Label("Normalizar Métricas:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            dcc.Checklist(
                                id=normalize_id,
                                options=[{"label": "", "value": "enabled"}],
                                value=["enabled"],
                                style={"minWidth": "100px"}
                            )
                        ],
                        style={"display": "flex", "flexDirection": "column", "marginRight": "15px"}
                    ),
                ],
            ),
            dcc.Loading(
                id=loading_id,
                type="circle",
                children=[
                    ddk.Graph(id=graph_id),
                ]
            ),
            html.Pre(id=error_id, style={"color": "red", "margin": "10px 0"}),
            ddk.CardFooter(title=description)
        ],
        width=50
    )

    test_inputs: dict[str, TestInput] = {
        port_selector_id: {
            "options": [port_options[i]["value"] for i in range(min(3, len(port_options)))],
            "default": port_default
        },
        normalize_id: {
            "options": [[], ["enabled"]],
            "default": ["enabled"]
        }
    }

    return {
        "layout": layout,
        "test_inputs": test_inputs
    }


def _update_logic(**kwargs) -> go.Figure:
    """Lógica principal del radar de desempeño de puertos (sin manejo de errores)."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Sin datos disponibles",
            annotations=[{
                "text": "No hay datos para mostrar",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    selected_ports = kwargs.get(f'{component_id}_ports', [])
    normalize = 'enabled' in kwargs.get(f'{component_id}_normalize', [])
    
    if not selected_ports:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Sin puertos seleccionados",
            annotations=[{
                "text": "Seleccione al menos un puerto para mostrar",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    # Filtrar puertos seleccionados
    df_filtered = df[df['ADUANA'].isin(selected_ports)]
    
    if len(df_filtered) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Sin datos para los puertos seleccionados",
            annotations=[{
                "text": "No hay datos disponibles para los puertos elegidos",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    # Agregar métricas por puerto
    port_metrics = df_filtered.groupby('ADUANA').agg({
        'total': 'sum',
        'kilo_neto': 'sum',
        'kilo_bruto': 'sum',
        'mercaderias_distintas': 'sum'
    }).reset_index()

    # Calcular métricas adicionales
    port_metrics['eficiencia_peso'] = port_metrics['kilo_neto'] / port_metrics['kilo_bruto']
    port_metrics['valor_por_kg'] = port_metrics['total'] / port_metrics['kilo_neto']

    logger.debug("Creando gráfico radar. port_metrics:\n%s", port_metrics.head())

    # Definir métricas para el radar
    metrics = ['total', 'kilo_neto', 'mercaderias_distintas', 'eficiencia_peso', 'valor_por_kg']
    metric_labels = ['Valor Total (Gs)', 'Peso Neto (kg)', 'Cantidad de Mercaderías', 'Eficiencia de Peso', 'Valor por kg (Gs/kg)']

    # Normalizar métricas si se solicita
    if normalize:
        for metric in metrics:
            max_val = port_metrics[metric].max()
            min_val = port_metrics[metric].min()
            if max_val != min_val:
                port_metrics[f'{metric}_norm'] = (port_metrics[metric] - min_val) / (max_val - min_val)
            else:
                port_metrics[f'{metric}_norm'] = 1.0
        metrics_to_plot = [f'{metric}_norm' for metric in metrics]
    else:
        metrics_to_plot = metrics

    # Crear radar
    fig = go.Figure()

    for port in selected_ports:
        port_data = port_metrics[port_metrics['ADUANA'] == port]
        if len(port_data) == 0:
            continue
            
        values = [port_data[metric].iloc[0] for metric in metrics_to_plot]
        values.append(values[0])  # cerrar el radar
        
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=metric_labels + [metric_labels[0]],
            fill='toself',
            name=port,
            line=dict(width=2),
            opacity=0.7
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1] if normalize else None
            )
        ),
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        )
    )

    return fig


@callback(
    output=[
        Output(f"{component_id}_graph", "figure"),
        Output(f"{component_id}_error", "children")
    ],
    inputs={
        f'{component_id}_ports': Input(f"{component_id}_ports", "value"),
        f'{component_id}_normalize': Input(f"{component_id}_normalize", "value"),
        **FILTER_CALLBACK_INPUTS
    }
)
def update(**kwargs) -> Tuple[go.Figure, str]:
    empty_fig = go.Figure()
    empty_fig.update_layout(
        title="Error en el gráfico",
        annotations=[{"text": "Ocurrió un error al actualizar este gráfico", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error actualizando el gráfico: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return empty_fig, error_msg


if __name__ == "__main__":
    import os
    app.run_server(
        debug=True,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8050))
    )
