import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image
import scrapers.data_wrangling as data_wrangling
from urllib.request import urlopen
import matplotlib.pyplot as plt
import numpy as np
from highlight_text import fig_text
from mplsoccer import Bumpy, FontManager, add_image, PyPizza

def overall():
    data = data_wrangling.draft_standings()
    data = data.sort_values(by='Points')

    fig = plt.figure(figsize=(6,2), dpi=300)
    ax = plt.subplot()

    ncols = data.shape[1]
    nrows = data.shape[0]

    ax.set_xlim(0, ncols + 1)
    ax.set_ylim(0, nrows)

    positions = [0.25, 4.25, 8.25, 12.25, 16.25, 20.25]
    columns = ['Team', 'Points', 'Goals Scored', 'Assists', 'Clean Sheets','Best Player']

    # Add table's main text
    for i in range(nrows):
        for j, column in enumerate(columns):
            if j == 0:
                ha = 'left'
            else:
                ha = 'center'
            if column == 'Points':
                text_label = f'{data[column].iloc[i]:,.0f}'
                weight = 'bold'
            else:
                text_label = f'{data[column].iloc[i]}'
                weight = 'normal'
            ax.annotate(
                xy=(positions[j], i + .5),
                text=text_label,
                ha=ha,
                va='center',
                weight=weight,
                color='white'
            )

    # Add column names
    column_names = ['Team', 'Points', 'Goals\nScored', 'Assists', 'Clean\nSheets','Best\nPlayer']
    for index, c in enumerate(column_names):
            if index == 0:
                ha = 'left'
            else:
                ha = 'center'
            ax.annotate(
                xy=(positions[index], nrows),
                text=column_names[index],
                ha=ha,
                va='bottom',
                weight='bold',
                color='white'
            )

    # Add dividing lines
    ax.plot([ax.get_xlim()[0], ax.get_xlim()[1]], [nrows, nrows], lw=1.5, color='white', marker='', zorder=4)
    ax.plot([ax.get_xlim()[0], ax.get_xlim()[1]], [0, 0], lw=1.5, color='white', marker='', zorder=4)
    for x in range(1, nrows):
        ax.plot([ax.get_xlim()[0], ax.get_xlim()[1]], [x, x], lw=1.15, color='gray', ls=':', zorder=3 , marker='')

    ax.set_axis_off()
    plt.savefig(
        'data_viz/figures/overall_table.png',
        dpi=300,
        transparent=True,
        bbox_inches='tight'
    )

def week_wise():
    font_normal = FontManager("https://raw.githubusercontent.com/google/fonts/main/apache/"
                            "roboto/Roboto%5Bwdth,wght%5D.ttf")
    font_bold = FontManager("https://raw.githubusercontent.com/google/fonts/main/apache/"
                            "robotoslab/RobotoSlab%5Bwght%5D.ttf")
    epl = Image.open(
        urlopen("https://raw.githubusercontent.com/andrewRowlinson/mplsoccer-assets/main/epl.png")
    )

    data = data_wrangling.merge_data()
    standings = data[data.played == True].groupby(by=['team_id','gw']).sum(numeric_only=True).reset_index()
    standings['overall_points'] = standings['stats.total_points'].groupby(standings['team_id']).transform('cumsum')
    standings['overall_rank'] = standings.groupby('gw')['overall_points'].rank(method='first', ascending=False).astype(int)
    players = {545927:'Nicolaj',546201:'Jesus',525936:'Kris',527284:'Mattia',524333:'Ollie'}
    standings.replace({'team_id':players}, inplace=True)
    draft_season_dict = {}
    for name in players.values():
        data = standings[standings.team_id == name]
        draft_season_dict[name] = data.overall_rank.to_list()

    # match-week
    match_day = ["Week " + str(num) for num in range(1, 39)]

    # highlight dict --> team to highlight and their corresponding colors
    highlight_dict = {
        "Nicolaj": "gold",
        "Jesus": "maroon",
        "Kris": "dodgerblue",
        "Ollie": "red",
        "Mattia": "white"
    }

    # instantiate object
    bumpy = Bumpy(
        scatter_color="#282A2C", line_color="#252525",  # scatter and line colors
        rotate_xticks=90,  # rotate x-ticks by 90 degrees
        ticklabel_size=17, label_size=30,  # ticklable and label font-size
        scatter_primary='D',  # marker to be used
        show_right=True,  # show position on the rightside
        plot_labels=True,  # plot the labels
        alignment_yvalue=0.5,  # y label alignment
        alignment_xvalue=0.5  # x label alignment
    )

    # plot bumpy chart
    fig, ax = bumpy.plot(
        x_list=match_day,  # match-day or match-week
        y_list=np.linspace(1, 6, 6).astype(int),  # position value from 1 to 20
        values=draft_season_dict,  # values having positions for each team
        secondary_alpha=0.5,   # alpha value for non-shaded lines/markers
        highlight_dict=highlight_dict,  # team to be highlighted with their colors
        figsize=(20, 10),  # size of the figure
        x_label='Week', y_label='Position',  # label name
        ylim=(0, 7),  # y-axis limit
        lw=2.5,   # linewidth of the connecting lines
        fontproperties=font_normal.prop,   # fontproperties for ticklables/labels
    )

    # title and subtitle
    TITLE = "Draft Premier League 2022/23 week-wise standings:"
    SUB_TITLE = "<Nicolaj>, <Jesus>, <Kris>, <Ollie>, <Mattia>"

    # add title
    fig.text(0.09, 0.95, TITLE, size=29, color="#F2F2F2", fontproperties=font_bold.prop)

    # add subtitle
    fig_text(
        0.09, 0.94, SUB_TITLE, color="#F2F2F2",
        highlight_textprops=[{"color": 'gold'}, {"color": 'maroon'}, {"color": 'dodgerblue'}, {"color": 'red'}, {"color": 'white'}],
        size=25, fig=fig, fontproperties=font_bold.prop
    )

    # add image
    fig = add_image(
        epl,
        fig,  # figure
        0.02, 0.9,  # left and bottom dimensions
        0.08, 0.08  # height and width values
    )

    # if space is left in the plot use this
    plt.tight_layout(pad=0.5)
    plt.savefig(
        'data_viz/figures/weekwise_table.png',
        dpi=300,
        bbox_inches='tight'
    )

def pizza_chart(player_name, stats, comparison, competition, gk=False):

    font_normal = FontManager('https://raw.githubusercontent.com/google/fonts/main/apache/roboto/'
                          'Roboto%5Bwdth,wght%5D.ttf')
    font_italic = FontManager('https://raw.githubusercontent.com/google/fonts/main/apache/roboto/'
                            'Roboto-Italic%5Bwdth,wght%5D.ttf')
    font_bold = FontManager('https://raw.githubusercontent.com/google/fonts/main/apache/robotoslab/'
                            'RobotoSlab%5Bwght%5D.ttf')

    # parameter list
    params = [i.replace(" ","\n",1) if len(i) > 10 else i for i in stats.Statistic.to_list()]

    # value list
    values = stats.Percentile.reset_index(drop=True)

    # color for the slices and text
    if gk == False:
        slice_colors = ["#1A78CF"] * 6 + ["#FF9300"] * 8 + ["#D70232"] * 5
        text_colors = ["#000000"] * 14 + ["#F2F2F2"] * 5
    if gk == True:
        slice_colors = ["#1A78CF"] * 6 + ["#FF9300"] * 4 + ["#D70232"] * 3
        text_colors = ["#000000"] * 10 + ["#F2F2F2"] * 3
    # instantiate PyPizza class
    baker = PyPizza(
        params=params,                  # list of parameters
        background_color="#222222",     # background color
        straight_line_color="#000000",  # color for straight lines
        straight_line_lw=1,             # linewidth for straight lines
        last_circle_color="#000000",    # color for last line
        last_circle_lw=1,               # linewidth of last circle
        other_circle_lw=0,              # linewidth for other circles
        inner_circle_size=15            # size of inner circle
    )

    # plot pizza
    fig, ax = baker.make_pizza(
        values,                          # list of values
        figsize=(8, 8.5),                # adjust the figsize according to your need
        color_blank_space="same",        # use the same color to fill blank space
        slice_colors=slice_colors,       # color for individual slices
        value_colors=text_colors,        # color for the value-text
        value_bck_colors=slice_colors,   # color for the blank spaces
        blank_alpha=0.4,                 # alpha for blank-space colors
        kwargs_slices=dict(
            edgecolor="#000000", zorder=2, linewidth=1
        ),                               # values to be used when plotting slices
        kwargs_params=dict(
            color="#F2F2F2", fontsize=11,
            fontproperties=font_normal.prop, va="center"
        ),                               # values to be used when adding parameter labels
        kwargs_values=dict(
            color="#F2F2F2", fontsize=11,
            fontproperties=font_normal.prop, zorder=3,
            bbox=dict(
                edgecolor="#000000", facecolor="cornflowerblue",
                boxstyle="round,pad=0.2", lw=1
            )
        )                                # values to be used when adding parameter-values labels
    )

    # add title
    fig.text(
        0.515, 0.975, f"{player_name}", size=16,
        ha="center", fontproperties=font_bold.prop, color="#F2F2F2"
    )

    # add subtitle
    fig.text(
        0.515, 0.955,
        f"Percentile Rank {comparison} | {competition}",
        size=13,
        ha="center", fontproperties=font_bold.prop, color="#F2F2F2"
    )

    # add credits
    CREDIT_1 = "Data: Opta via FBref"

    fig.text(
        0.99, 0.02, f"{CREDIT_1}", size=9,
        fontproperties=font_italic.prop, color="#F2F2F2",
        ha="right"
    )

    # add text
    if gk == False:
        fig.text(
            0.34, 0.93, "Attacking        Possession       Defending", size=14,
            fontproperties=font_bold.prop, color="#F2F2F2"
        )
        # add rectangles
        fig.patches.extend([
            plt.Rectangle(
                (0.31, 0.9225), 0.025, 0.021, fill=True, color="#1a78cf",
                transform=fig.transFigure, figure=fig
            ),
            plt.Rectangle(
                (0.462, 0.9225), 0.025, 0.021, fill=True, color="#ff9300",
                transform=fig.transFigure, figure=fig
            ),
            plt.Rectangle(
                (0.632, 0.9225), 0.025, 0.021, fill=True, color="#d70232",
                transform=fig.transFigure, figure=fig
            ),
        ])
    if gk == True:
        fig.text(
            0.25, 0.93, "Shot Stopping             Distribution       Sweeping", size=14,
            fontproperties=font_bold.prop, color="#F2F2F2"
        )
        # add rectangles
        fig.patches.extend([
            plt.Rectangle(
                (0.22, 0.9225), 0.025, 0.021, fill=True, color="#1a78cf",
                transform=fig.transFigure, figure=fig
            ),
            plt.Rectangle(
                (0.462, 0.9225), 0.025, 0.021, fill=True, color="#ff9300",
                transform=fig.transFigure, figure=fig
            ),
            plt.Rectangle(
                (0.632, 0.9225), 0.025, 0.021, fill=True, color="#d70232",
                transform=fig.transFigure, figure=fig
            ),
        ])
    return fig

def main():
    overall()
    week_wise()