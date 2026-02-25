import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "notebook"

# Cycle through a palette if you need more than 26
def __get_sankey_colors__(n, palette = "Vivid"):
    base_colors = getattr(px.colors.qualitative, palette)
    # Repeat palette if needed
    return [base_colors[i % len(base_colors)] for i in range(n)]


def get_sankey_figure(mp, pad = 15, thickness = 20, line_color = "black", line_width = 0.5, title_text = "Basic Sankey", font_size = 10):
    # Define nodes and links
    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = pad,
          thickness = thickness,
          line = dict(color = line_color, width = line_width),
          label = mp["label"],
          color = __get_sankey_colors__(len(mp["label"]))
        ),
        link = dict(
          source = mp["source"],
          target = mp["target"],
          value = mp["value"]
      ))])

    # update layout
    fig.update_layout(title_text = title_text, font_size = font_size)

    # return
    return fig


