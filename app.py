import json
import pandas as pd
import numpy as np
from logger import logger

from flask_caching import Cache

cache = Cache()

@cache.memoize()
def get_data(data_path='data/data.parquet'):
    # Define explicit type mappings for all columns
    type_mapping = {
        # String columns (keep as strings initially)
        "ADUANA": str,

        # Numeric columns (use nullable types for mixed data)
        "kilo_neto": "Float64",
        "kilo_bruto": "Float64", 
        "total": "Float64",
        "mercaderias_distintas": "Int64",
    }

    # Define column-specific values to treat as NaN (for mixed type columns)
    na_values_mapping = {
        # No special na_values needed for this dataset
    }
    
    # Load data based on file extension
    if data_path.endswith('.parquet'):
        # Read Parquet files
        df = pd.read_parquet(data_path, engine='pyarrow')
    else:
        # Read CSV with explicit type mapping and column-specific na_values and automatic separator detection
        df = pd.read_csv(data_path, dtype=type_mapping, na_values=na_values_mapping, sep=None, engine="python", encoding="utf-8-sig")
    
    logger.debug("Data loaded. Shape: %s", df.shape)
    logger.debug("Sample data:\n%s", df.head())

    # Filter out rows where all values are null
    df = df.dropna(how='all')

    logger.debug("Data after filtering nulls. Shape: %s", df.shape)

    return df


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
    app_title = "Paraguay Import Ports Analysis"
    app_description = "Data app analyzing import activity across Paraguay's customs ports, showing rankings and comparisons of physical volume, monetary value, and merchandise diversity by port."
    app_logo = "https://dash.plotly.com/assets/images/plotly_logo_dark.png"
    app_tags = [
        ddk.Tag(text="**Data Updated:** 2025-09-19", icon="circle-check"),
        ddk.Tag(text="**Created by:** Plotly Studio", icon="user"),
        ddk.Tag(text="**Data Source**: Paraguay Import Ports Analysis data", icon="database"),
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
                        title="Port Selection",
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
                        title="Top Performing Ports",
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
                        title="Port Type",
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
                        title="Total Import Value Range (Guaraníes)"
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
                        title="Net Weight Range (Kilograms)"
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
                        title="Gross Weight Range (Kilograms)"
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
                        title="Merchandise Diversity Range"
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
                label='Total Ports',
                width=20
            ),
            ddk.DataCard(
                id='total_weight_card',
                value='...',
                label='Total Weight (tons)',
                width=20
            ),
            ddk.DataCard(
                id='avg_weight_card',
                value='...',
                label='Avg Weight per Port (tons)',
                width=20
            ),
            ddk.DataCard(
                id='top_port_card',
                value='...',
                label='Top Port by Weight',
                width=20
            ),
            ddk.DataCard(
                id='total_value_card',
                value='...',
                label='Total Value (USD)',
                width=20
            )
        ])

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
        
        # Total weight in tons (kilo_neto is in kg, convert to tons)
        total_weight_tons = df["kilo_neto"].sum() / 1000
        
        # Average weight per port in tons
        avg_weight_tons = total_weight_tons / total_ports if total_ports > 0 else 0
        
        # Find top port by weight
        top_port = df.loc[df["kilo_neto"].idxmax(), "ADUANA"]
        
        # Total value
        total_value = df["total"].sum()

        # Format the results with appropriate number formatting
        total_ports_formatted = f"{total_ports:,}"
        total_weight_formatted = f"{total_weight_tons:,.0f}"
        avg_weight_formatted = f"{avg_weight_tons:,.0f}"
        top_port_formatted = str(top_port)[:20] + "..." if len(str(top_port)) > 20 else str(top_port)
        total_value_formatted = f"${total_value:,.0f}"

        logger.debug("Calculated metrics - Ports: %s, Weight: %s tons, Top port: %s", 
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
            "headerName": "Rank (Total Value)",
            "field": "total_rank",
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "width": 150,
            "pinned": "left"
        },
        {
            "headerName": "Port/Customs (Aduana)",
            "field": "ADUANA",
            "filter": "agTextColumnFilter",
            "width": 200,
            "pinned": "left"
        },
        {
            "headerName": "Total Value",
            "field": "total",
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
            "width": 150
        },
        {
            "headerName": "Net Weight (kg)",
            "field": "kilo_neto", 
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
            "width": 150
        },
        {
            "headerName": "Gross Weight (kg)",
            "field": "kilo_bruto",
            "type": "numericColumn", 
            "filter": "agNumberColumnFilter",
            "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
            "width": 150
        },
        {
            "headerName": "Merchandise Types",
            "field": "mercaderias_distintas",
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
            "width": 150
        },
        {
            "headerName": "Weight Rank",
            "field": "weight_rank",
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "width": 120
        },
        {
            "headerName": "Diversity Rank", 
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
        {"label": "Descending (Highest to Lowest)", "value": "desc"},
        {"label": "Ascending (Lowest to Highest)", "value": "asc"}
    ]
    sort_order_default = "desc"

    title = "Port Ranking by Total Import Value"
    description = "Ranking of ports by total import value showing the most important ports for Paraguay's import operations"

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap", "rowGap": "10px", "alignItems": "center", "marginBottom": "15px"},
                children=[
                    html.Div(
                        children=[
                            html.Label("Number of Ports:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
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
                            html.Label("Sort Order:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
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
    """Core chart update logic without error handling."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="No data available",
            annotations=[{
                "text": "No data is available to display",
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

    logger.debug("Starting chart creation. df:\n%s", df.head())
    logger.debug("Top N: %s, Sort order: %s", top_n, sort_order)

    port_totals = df.groupby('ADUANA')['total'].sum().reset_index()
    
    ascending = sort_order == "asc"
    port_totals = port_totals.sort_values('total', ascending=ascending)
    
    port_totals = port_totals.head(top_n)
    
    if sort_order == "desc":
        port_totals = port_totals.sort_values('total', ascending=True)
    else:
        port_totals = port_totals.sort_values('total', ascending=False)

    port_totals['total_billions'] = port_totals['total'] / 1e12

    logger.debug("Port totals after processing:\n%s", port_totals.head())

    fig = px.bar(
        port_totals,
        x='total_billions',
        y='ADUANA',
        orientation='h',
        labels={
            'total_billions': 'Total Import Value (Trillions)',
            'ADUANA': 'Port/Customs Office'
        }
    )

    fig.update_traces(
        hovertemplate="<b>%{y}</b><br>Total Value: %{x:.2f} Trillion<extra></extra>"
    )

    fig.update_layout(
        xaxis_title="Total Import Value (Trillions)",
        yaxis_title="Port/Customs Office",
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
        title="Error in chart",
        annotations=[{"text": "An error occurred while updating this chart", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error updating chart: {str(e)}\n{traceback.format_exc()}"
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

    weight_type_id = f"{component_id}_weight_type"
    weight_type_options = [
        {"label": "Net Weight", "value": "kilo_neto"},
        {"label": "Gross Weight", "value": "kilo_bruto"}
    ]
    weight_type_default = "kilo_neto"

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

    title = "Port Volume vs Monetary Value"
    description = "Scatter plot showing the relationship between port volume (weight) and monetary value, with point size representing merchandise diversity"

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap", "rowGap": "10px", "alignItems": "center", "marginBottom": "15px"},
                children=[
                    html.Div(
                        children=[
                            html.Label("Weight Type:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
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
                            html.Label("Point Size Factor:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
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
                        style={"display": "flex", "flexDirection": "column", "marginRight": "15px", "width": "300px"}
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
    """Core chart update logic without error handling."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="No data available",
            annotations=[{
                "text": "No data is available to display",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    weight_type = kwargs.get(f'{component_id}_weight_type', 'kilo_neto')
    size_factor = kwargs.get(f'{component_id}_size_factor', 1.0)
    
    if weight_type is None:
        weight_type = 'kilo_neto'
    if size_factor is None:
        size_factor = 1.0

    logger.debug("Starting chart creation. df:\n%s", df.head())
    logger.debug("Weight type: %s, Size factor: %s", weight_type, size_factor)

    # Convert numeric columns to ensure proper data types
    df['total'] = pd.to_numeric(df['total'], errors='coerce')
    df[weight_type] = pd.to_numeric(df[weight_type], errors='coerce')
    df['mercaderias_distintas'] = pd.to_numeric(df['mercaderias_distintas'], errors='coerce')

    # Remove any rows with null values in key columns
    df_clean = df.dropna(subset=['total', weight_type, 'mercaderias_distintas'])
    
    if len(df_clean) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="No valid data available",
            annotations=[{
                "text": "No valid data points after filtering",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    # Calculate size values based on merchandise diversity and size factor
    base_size = df_clean['mercaderias_distintas']
    # Normalize size to reasonable range (5-50 pixels) and apply size factor
    min_size = 5 * size_factor
    max_size = 50 * size_factor
    normalized_size = min_size + (max_size - min_size) * (base_size - base_size.min()) / (base_size.max() - base_size.min())

    # Create scatter plot
    fig = px.scatter(
        df_clean,
        x=weight_type,
        y='total',
        size=normalized_size,
        hover_name='ADUANA',
        hover_data={
            weight_type: ':,.0f',
            'total': ':,.0f',
            'mercaderias_distintas': ':,.0f'
        },
        labels={
            weight_type: f"Weight ({'Net' if weight_type == 'kilo_neto' else 'Gross'}) (kg)",
            'total': 'Monetary Value',
            'mercaderias_distintas': 'Merchandise Types'
        }
    )

    # Update layout
    weight_label = "Net Weight" if weight_type == "kilo_neto" else "Gross Weight"
    fig.update_layout(
        xaxis_title=f"{weight_label} (kg)",
        yaxis_title="Monetary Value",
        showlegend=False
    )

    # Format axes with appropriate number formatting
    fig.update_xaxes(tickformat='.2s')
    fig.update_yaxes(tickformat='.2s')

    # Update hover template for better formatting
    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>" +
                     f"{weight_label}: %{{x:,.0f}} kg<br>" +
                     "Monetary Value: %{y:,.0f}<br>" +
                     "Merchandise Types: %{marker.size:,.0f}<extra></extra>"
    )

    logger.debug("Chart created successfully with %d data points", len(df_clean))

    return fig

@callback(
    output=[
        Output(f"{component_id}_graph", "figure"),
        Output(f"{component_id}_error", "children")
    ],
    inputs={
        f'{component_id}_weight_type': Input(f"{component_id}_weight_type", "value"),
        f'{component_id}_size_factor': Input(f"{component_id}_size_factor", "value"),
        **FILTER_CALLBACK_INPUTS
    }
)
def update(**kwargs) -> Tuple[go.Figure, str]:
    empty_fig = go.Figure()
    empty_fig.update_layout(
        title="Error in chart",
        annotations=[{"text": "An error occurred while updating this chart", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error updating chart: {str(e)}\n{traceback.format_exc()}"
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
        {"label": "Merchandise Count", "value": "mercaderias_distintas"},
        {"label": "Total Value", "value": "total"},
        {"label": "Net Weight", "value": "kilo_neto"}
    ]
    sort_metric_default = "mercaderias_distintas"

    min_merchandise_id = f"{component_id}_min_merchandise"
    max_merchandise_id = f"{component_id}_max_merchandise"

    df = get_data()
    min_merchandise = int(df["mercaderias_distintas"].min())
    max_merchandise = int(df["mercaderias_distintas"].max())

    title = "Merchandise Diversity Ranking by Port"
    description = "Ranking of ports by merchandise diversity, total value, or net weight with filtering options"

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap", "rowGap": "10px", "alignItems": "center", "marginBottom": "15px"},
                children=[
                    html.Div(
                        children=[
                            html.Label("Sort by:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
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
                            html.Label("Merchandise Count Range:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
                            html.Div([
                                dcc.Input(
                                    id=min_merchandise_id,
                                    type="number",
                                    value=min_merchandise,
                                    min=min_merchandise,
                                    max=max_merchandise,
                                    placeholder="Min",
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
                                    placeholder="Max",
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
    """Core chart update logic without error handling."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="No data available",
            annotations=[{
                "text": "No data is available to display",
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
            title="No data available after filtering",
            annotations=[{
                "text": "No data matches the current filter criteria",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    df_sorted = df.sort_values(by=sort_metric, ascending=False)

    logger.debug("Starting chart creation. df:\n%s", df_sorted.head())

    metric_labels = {
        'mercaderias_distintas': 'Merchandise Count',
        'total': 'Total Value',
        'kilo_neto': 'Net Weight (kg)'
    }

    y_title = metric_labels.get(sort_metric, sort_metric)

    fig = px.bar(
        df_sorted,
        x='ADUANA',
        y=sort_metric,
        title="",
        labels={
            'ADUANA': 'Port/Customs Office',
            sort_metric: y_title
        }
    )

    fig.update_layout(
        xaxis_title="Port/Customs Office",
        yaxis_title=y_title,
        xaxis={'categoryorder': 'total descending'}
    )

    fig.update_xaxes(tickangle=45)

    if sort_metric == 'total':
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>" + 
                         f"{y_title}: %{{y:,.0f}}<extra></extra>"
        )
    elif sort_metric == 'kilo_neto':
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>" + 
                         f"{y_title}: %{{y:,.0f}}<extra></extra>"
        )
    else:
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>" + 
                         f"{y_title}: %{{y:,}}<extra></extra>"
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
        title="Error in chart",
        annotations=[{"text": "An error occurred while updating this chart", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error updating chart: {str(e)}\n{traceback.format_exc()}"
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

# Define defaults at module level
weight_type_default = "kilo_neto"

def component() -> ComponentResponse:
    graph_id = f"{component_id}_graph"
    error_id = f"{component_id}_error"
    loading_id = f"{component_id}_loading"

    weight_type_id = f"{component_id}_weight_type"
    weight_type_options = [
        {"label": "Net Weight", "value": "kilo_neto"},
        {"label": "Gross Weight", "value": "kilo_bruto"}
    ]

    title = "Port Weight Distribution"
    description = "Treemap visualization showing weight distribution across customs ports, with color coding by total import value"

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap", "rowGap": "10px", "alignItems": "center", "marginBottom": "15px"},
                children=[
                    html.Div(
                        children=[
                            html.Label("Weight Type:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
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
    """Core chart update logic without error handling."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            annotations=[{
                "text": "No data available",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    weight_type = kwargs.get(f'{component_id}_weight_type', weight_type_default)
    if weight_type is None:
        weight_type = weight_type_default

    # Aggregate data by port
    df_agg = df.groupby('ADUANA').agg({
        'kilo_neto': 'sum',
        'kilo_bruto': 'sum',
        'total': 'sum',
        'mercaderias_distintas': 'sum'
    }).reset_index()

    # Convert weight to millions of kilograms for better readability
    df_agg['weight_millions'] = df_agg[weight_type] / 1_000_000
    
    # Convert total value to billions for better readability
    df_agg['total_billions'] = df_agg['total'] / 1_000_000_000_000

    # Filter out ports with very small weights to keep treemap readable
    min_weight_threshold = df_agg['weight_millions'].quantile(0.05)
    df_filtered = df_agg[df_agg['weight_millions'] >= min_weight_threshold].copy()

    if len(df_filtered) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            annotations=[{
                "text": "No significant data to display",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    # Sort by weight for better treemap layout
    df_filtered = df_filtered.sort_values('weight_millions', ascending=False)

    logger.debug("Starting chart creation. df_filtered:\n%s", df_filtered.head())

    # Create treemap
    weight_label = "Net Weight" if weight_type == "kilo_neto" else "Gross Weight"
    
    fig = px.treemap(
        df_filtered,
        path=['ADUANA'],
        values='weight_millions',
        color='total_billions',
        color_continuous_scale='Viridis',
        hover_data={'weight_millions': ':.1f', 'total_billions': ':.2f', 'mercaderias_distintas': ':,'}
    )

    # Update hover template for better readability
    fig.update_traces(
        hovertemplate="<b>%{label}</b><br>" +
                     f"{weight_label}: %{{value:.1f}}M kg<br>" +
                     "Import Value: %{color:.2f}B<br>" +
                     "Distinct Products: %{customdata[2]:,}<extra></extra>",
        textinfo="label+value"
    )

    # Update layout
    fig.update_layout(
        coloraxis_colorbar=dict(
            title="Import Value<br>(Billions)",
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
        annotations=[{"text": "An error occurred while updating this chart", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error updating chart: {str(e)}\n{traceback.format_exc()}"
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

    # Get data to determine top ports
    df = get_data()
    top_ports = df.groupby('ADUANA')['total'].sum().nlargest(10).index.tolist()
    
    port_selector_id = f"{component_id}_ports"
    port_options = [{"label": port, "value": port} for port in top_ports]
    port_default = top_ports[:3] if len(top_ports) >= 3 else top_ports
    
    normalize_id = f"{component_id}_normalize"

    title = "Port Performance Radar Chart"
    description = "Multi-metric comparison of Paraguay's top ports showing total value, weight metrics, and merchandise diversity"

    layout = ddk.Card(
        id=component_id,
        children=[
            ddk.CardHeader(title=title),
            html.Div(
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap", "rowGap": "10px", "alignItems": "center", "marginBottom": "15px"},
                children=[
                    html.Div(
                        children=[
                            html.Label("Select Ports:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
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
                            html.Label("Normalize Metrics:", style={"marginBottom": "5px", "fontWeight": "bold", "display": "block"}),
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
    """Core chart update logic without error handling."""
    df = filter_data(get_data(), **kwargs)
    if len(df) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="No data available",
            annotations=[{
                "text": "No data is available to display",
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
            title="No ports selected",
            annotations=[{
                "text": "Please select at least one port to display",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    # Filter for selected ports
    df_filtered = df[df['ADUANA'].isin(selected_ports)]
    
    if len(df_filtered) == 0:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="No data for selected ports",
            annotations=[{
                "text": "No data available for the selected ports",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig

    # Aggregate metrics by port
    port_metrics = df_filtered.groupby('ADUANA').agg({
        'total': 'sum',
        'kilo_neto': 'sum',
        'kilo_bruto': 'sum',
        'mercaderias_distintas': 'sum'
    }).reset_index()

    # Calculate additional metrics
    port_metrics['weight_efficiency'] = port_metrics['kilo_neto'] / port_metrics['kilo_bruto']
    port_metrics['value_per_kg'] = port_metrics['total'] / port_metrics['kilo_neto']

    logger.debug("Starting chart creation. port_metrics:\n%s", port_metrics.head())

    # Define metrics for radar chart
    metrics = ['total', 'kilo_neto', 'mercaderias_distintas', 'weight_efficiency', 'value_per_kg']
    metric_labels = ['Total Value', 'Net Weight (kg)', 'Merchandise Types', 'Weight Efficiency', 'Value per kg']

    # Normalize metrics if requested
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

    # Create radar chart
    fig = go.Figure()

    # Add trace for each port
    for i, port in enumerate(selected_ports):
        port_data = port_metrics[port_metrics['ADUANA'] == port]
        if len(port_data) == 0:
            continue
            
        values = []
        for metric in metrics_to_plot:
            val = port_data[metric].iloc[0]
            values.append(val)
        
        # Close the radar chart by adding the first value at the end
        values.append(values[0])
        
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=metric_labels + [metric_labels[0]],
            fill='toself',
            name=port,
            line=dict(width=2),
            opacity=0.7
        ))

    # Update layout
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
        title="Error in chart",
        annotations=[{"text": "An error occurred while updating this chart", "showarrow": False, "font": {"size": 20}}]
    )

    try:
        figure = _update_logic(**kwargs)
        return figure, ""

    except Exception as e:
        error_msg = f"Error updating chart: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return empty_fig, error_msg