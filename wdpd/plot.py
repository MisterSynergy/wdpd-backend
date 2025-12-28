from glob import glob
import logging
from math import ceil as m_ceil
from typing import Optional, TypedDict

from matplotlib import cm
from matplotlib.colors import LogNorm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from numpy import array as np_array, amax
import pandas as pd

from .config import PLOT_WINDOW_DAYS, PLOTPATH, FIGSIZE_STANDARD, FIGSIZE_TALL, FIGSIZE_WIDE, FIGSIZE_HEATMAP, \
    QID_BIN_SIZE, QID_BIN_MAX, ORES_MODELS
from .helper import delete_file, wdqs_query


LOG = logging.getLogger(__name__)


class PlotParamsDict(TypedDict):
    filter_window : pd.Series
    xticks_window : list[pd.Timestamp]
    xticklabels_window : list[str]


class Plot:
    def __init__(self, filename:Optional[str]=None, figsize:Optional[tuple[float, float]]=None, svg:bool=True):
        self.filename = filename
        if figsize is None:
            figsize = FIGSIZE_STANDARD
        self.fig, self.ax = plt.subplots(nrows=1, ncols=1, figsize=figsize)
        self.svg = svg


    def __enter__(self) -> tuple[Figure, Axes]:
        return (self.fig, self.ax)


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fig.tight_layout()
        if self.filename is not None:
            self.fig.savefig(f'{self.filename}.png')
            if self.svg is True:
                self.fig.savefig(f'{self.filename}.svg')
        plt.close(self.fig)


def plot_edits_by_date(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> int:
    filename = f'{PLOTPATH}editsByDate'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id']].groupby(
            by=[
                unpatrolled_changes['time'].dt.date,
                unpatrolled_changes['actor_user'].isna()
            ]
        ).count()
        tmp.groupby(level=0).sum().plot(kind='line', grid=True, ax=ax)
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['total unpatrolled changes', 'by registered users', 'by IP users'])
        ax.set_xlabel('date')
        ax.set_ylabel('number of changes per day')
        _, _, _, ymax = ax.axis()
        ymax = int(ymax)
        ax.set(ylim=(0, ymax))
        ax.set_xticks([ mdates.date2num(ts.to_pydatetime()) for ts in plot_params['xticks_window'] ])
        ax.set_xticklabels(plot_params['xticklabels_window'])

    LOG.info('Plotted edits by date')

    return ymax


def plot_edits_by_weekday(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict, ymax:Optional[int]=None) -> None:
    filename = f'{PLOTPATH}editsByWeekday'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id']].groupby(
            by=[
                unpatrolled_changes['time'].dt.weekday,
                unpatrolled_changes['actor_user'].isna()
            ]
        ).count().div(
            other=PLOT_WINDOW_DAYS / 7,
            axis=0
        )
        tmp.groupby(level=0).sum().plot(kind='line', grid=True, ax=ax)
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['total unpatrolled changes', 'by registered users', 'by IP users'])
        ax.set_xlabel('weekday')
        ax.set_ylabel('number of changes per day (weekday avg)')
        if ymax is not None:
            ax.set(ylim=(0, ymax))
        ax.set_xticks(range(0, 7, 1))
        ax.set_xticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    LOG.info('Plotted edits by weekday')


def plot_edits_by_hour(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> None:
    filename = f'{PLOTPATH}editsByHour'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id']].groupby(
            by=[
                unpatrolled_changes['time'].dt.hour,
                unpatrolled_changes['actor_user'].isna()
            ]
        ).count().div(
            other=PLOT_WINDOW_DAYS,
            axis=0
        )
        tmp.groupby(level=0).sum().plot(kind='line', grid=True, ax=ax)
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['total unpatrolled changes', 'by registered users', 'by IP users'])
        ax.set_xlabel('hour of day (UTC)')
        ax.set_ylabel(f'number of changes per hour ({PLOT_WINDOW_DAYS}d avg)')
        ax.set_xticks(range(0, 25, 3))
        _, _, _, ymax = ax.axis()
        ax.set(xlim=(0, 24), ylim=(0, ymax))

    LOG.info('Plotted edits by hour')


def plot_patrol_status_by_date(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> int:
    filename = f'{PLOTPATH}patrolstatusByDate'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id']].groupby(
            by=[
                unpatrolled_changes['time'].dt.date,
                unpatrolled_changes['rc_patrolled']
            ]
        ).count()
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['still unpatrolled', 'manually patrolled'])
        ax.set_xlabel('date')
        ax.set_ylabel('number of changes per day')
        _, _, _, ymax = ax.axis()
        ymax = int(ymax)
        ax.set(ylim=(0, ymax))
        ax.set_xticks([ mdates.date2num(ts.to_pydatetime()) for ts in plot_params['xticks_window'] ])
        ax.set_xticklabels(plot_params['xticklabels_window'])

    LOG.info('Plotted patrol status by date')

    return ymax


def plot_patrol_status_by_weekday(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict, ymax:Optional[int]=None) -> None:
    filename = f'{PLOTPATH}patrolstatusByWeekday'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id']].groupby(
            by=[
                unpatrolled_changes['time'].dt.weekday,
                unpatrolled_changes['rc_patrolled']
            ]
        ).count().div(
            other=PLOT_WINDOW_DAYS / 7,
            axis=1
        )
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['still unpatrolled', 'manually patrolled'])
        ax.set_xlabel('weekday')
        ax.set_ylabel('number of changes per day (weekday avg)')
        if ymax is not None:
            ax.set(ylim=(0, ymax))
        ax.set_xticks(range(0, 7, 1))
        ax.set_xticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    LOG.info('Plotted patrol status by weekday')


def plot_patrol_status_by_hour(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> None:
    filename = f'{PLOTPATH}patrolstatusByHour'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id']].groupby(
            by=[
                unpatrolled_changes['time'].dt.hour,
                unpatrolled_changes['rc_patrolled']
            ]
        ).count().div(
            other=PLOT_WINDOW_DAYS,
            axis=1
        )
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['still unpatrolled', 'manually patrolled'])
        ax.set_xlabel('hour of day (UTC)')
        ax.set_ylabel(f'number of changes per hour ({PLOT_WINDOW_DAYS}d avg)')
        ax.set_xticks(range(0, 25, 3))
        _, _, _, ymax = ax.axis()
        ax.set(xlim=(0, 24), ylim=(0, ymax))

    LOG.info('Plotted patrol status by hour')


def plot_editor_status_by_date(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> int:
    filename = f'{PLOTPATH}editorstatusByDate'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id', 'actor_name']].groupby(
            by=[
                unpatrolled_changes['time'].dt.date,
                unpatrolled_changes['actor_user'].notna()
            ]
        )['actor_name'].nunique()
        tmp.groupby(level=0).sum().plot(kind='line', grid=True, ax=ax)
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['all types', '# of IPs', '# of registered users'])
        ax.set_xlabel('date')
        ax.set_ylabel('number of editors per day')
        _, _, _, ymax = ax.axis()
        ymax = int(ymax)
        ax.set(ylim=(0, ymax))
        ax.set_xticks([ mdates.date2num(ts.to_pydatetime()) for ts in plot_params['xticks_window'] ])
        ax.set_xticklabels(plot_params['xticklabels_window'])

    LOG.info('Plotted editor status by date')

    return ymax


def plot_editor_status_by_weekday(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict, ymax:Optional[int]=None) -> None:
    filename = f'{PLOTPATH}editorstatusByWeekday'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id', 'actor_name']].groupby(
            by=[
                unpatrolled_changes['time'].dt.weekday,
                unpatrolled_changes['actor_user'].notna()
            ]
        )['actor_name'].nunique().div(
            other=PLOT_WINDOW_DAYS / 7,
            axis=0
        )
        tmp.groupby(level=0).sum().plot(kind='line', grid=True, ax=ax)
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['all types', '# of IPs', '# of registered users'])
        ax.set_xlabel('weekday')
        ax.set_ylabel('number of editors per day (weekday avg)')
        if ymax is not None:
            ax.set(ylim=(0, ymax))
        ax.set_xticks(range(0, 7, 1))
        ax.set_xticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    LOG.info('Plotted editor status by weekday')


def plot_editor_status_by_hour(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> None:
    filename = f'{PLOTPATH}editorstatusByHour'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id', 'actor_name']].groupby(
            by=[
                unpatrolled_changes['time'].dt.hour,
                unpatrolled_changes['actor_user'].notna()
            ]
        )['actor_name'].nunique().div(
            other=PLOT_WINDOW_DAYS,
            axis=0
        )
        tmp.groupby(level=0).sum().plot(kind='line', grid=True, ax=ax)
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['all types', '# of IPs', '# of registered users'])
        ax.set_xlabel('hour of day (UTC)')
        ax.set_ylabel(f'number of editors per hour ({PLOT_WINDOW_DAYS}d avg)')
        ax.set_xticks(range(0, 25, 3))
        _, _, _, ymax = ax.axis()
        ax.set(xlim=(0, 24), ylim=(0, ymax))

    LOG.info('Plotted editor status by hour')


def plot_unpatrolled_actions_by_date(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> None:
    filename = f'{PLOTPATH}actionsByDate'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id']].groupby(
            by=[
                unpatrolled_changes['time'].dt.date,
                unpatrolled_changes['rc_source']
            ]
        ).count()
        tmp.groupby(level=0).sum().plot(kind='line', grid=True, ax=ax)
        tmp.unstack(level=1).plot(kind='line', grid=True, ax=ax)

        ax.legend(['total changes', 'unpatrolled edit item', 'unpatrolled create item'])
        ax.set_xlabel('date')
        ax.set_ylabel('number of changes')
        _, _, _, ymax = ax.axis()
        ax.set(ylim=(0, ymax))
        ax.set_xticks([ mdates.date2num(ts.to_pydatetime()) for ts in plot_params['xticks_window'] ])
        ax.set_xticklabels(plot_params['xticklabels_window'])

    LOG.info('Plotted unpatrolled actions by date')


def plot_reverted_by_date(unpatrolled_changes:pd.DataFrame, change_tags:pd.DataFrame, plot_params:PlotParamsDict) -> None:
    filename = f'{PLOTPATH}revertedByDate'

    undone = ['mw-reverted']

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp_rev_1 = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id', 'time']].merge(
            right=change_tags.loc[(change_tags['ctd_name'].isin(undone)), ['rc_id', 'ctd_name']],
            on='rc_id'
        )
        tmp_rev_1a = tmp_rev_1.groupby(by=tmp_rev_1['time'].dt.date).count()
        tmp_rev_2 = unpatrolled_changes.groupby(by=unpatrolled_changes['time'].dt.date).count()
        tmp_rev_full = tmp_rev_1a[['ctd_name']].join(other=tmp_rev_2[['rc_id']])
        del tmp_rev_1, tmp_rev_1a, tmp_rev_2

        tmp_rev_full.plot(y=['ctd_name', 'rc_id'], kind='line', grid=True, ax=ax)

        ax.legend(['reverted (lower bound)', 'not reverted (upper bound)'])
        ax.set_xlabel('date')
        ax.set_ylabel('number of changes')
        _, _, _, ymax = ax.axis()
        ax.set(ylim=(0, ymax))
        ax.set_xticks([ mdates.date2num(ts.to_pydatetime()) for ts in plot_params['xticks_window'] ])
        ax.set_xticklabels(plot_params['xticklabels_window'])

    LOG.info('Plotted reverted edits by date')


def plot_qid_bin_by_revisions(unpatrolled_changes:pd.DataFrame) -> None:
    filename = f'{PLOTPATH}qidBinRev'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp_unpatrolled = unpatrolled_changes.loc[(unpatrolled_changes['rc_patrolled']==0 & unpatrolled_changes['reverted'].isna()), ['num_title']].groupby(by=unpatrolled_changes['num_title'].floordiv(other=QID_BIN_SIZE)).count()
        tmp_patrolled = unpatrolled_changes.loc[(unpatrolled_changes['rc_patrolled']==1 & unpatrolled_changes['reverted'].isna()), ['num_title']].groupby(by=unpatrolled_changes['num_title'].floordiv(other=QID_BIN_SIZE)).count()
        tmp_reverted =  unpatrolled_changes.loc[~unpatrolled_changes['reverted'].isna(), ['num_title']].groupby(by=unpatrolled_changes['num_title'].floordiv(other=QID_BIN_SIZE)).count()
        tmp_unpatrolled.rename(columns={'num_title' : 'cnt_unpatrolled'}, inplace=True)
        tmp_patrolled.rename(columns={'num_title' : 'cnt_patrolled'}, inplace=True)
        tmp_reverted.rename(columns={'num_title' : 'cnt_reverted'}, inplace=True)
        tmp = tmp_unpatrolled.merge(right=tmp_patrolled, on='num_title', how='left').merge(right=tmp_reverted, on='num_title', how='left')

        tmp.plot.bar(stacked=True,grid=True, ax=ax, width=1)
        ax.legend(['still unpatrolled', 'manually patrolled', 'reverted'])
        ax.set_xlabel('Q-ID bin (1M item bins)')
        ax.set_ylabel('number of changes')
        ax.set_xticks([0, 20, 40, 60, 80, 100, 120])
        ax.set_xticklabels(['Q0', 'Q20M', 'Q40M', 'Q60M', 'Q80M', 'Q100M', 'Q120M'], rotation=0)
        #xmin, xmax, ymin, ymax = ax.axis()
        ax.set(xlim=(-0.5, QID_BIN_MAX/QID_BIN_SIZE+0.5))

    LOG.info('Plotted QID bins by revisions')


def plot_qid_bin_by_item(unpatrolled_changes:pd.DataFrame) -> None:
    filename = f'{PLOTPATH}qidBinQid'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp =  unpatrolled_changes[['num_title']].drop_duplicates().groupby(
            by=unpatrolled_changes['num_title'].floordiv(other=QID_BIN_SIZE)
        ).count()
        tmp.plot.bar(stacked=True, grid=True, ax=ax, width=1)

        ax.legend(['items with unpatrolled changes'])
        ax.set_xlabel('Q-ID bin (1M item bins)')
        ax.set_ylabel('number of items with changes')
        ax.set_xticks([0, 20, 40, 60, 80, 100, 120])
        ax.set_xticklabels(['Q0', 'Q20M', 'Q40M', 'Q60M', 'Q80M', 'Q100M', 'Q120M'], rotation=0)
        ax.set(xlim=(-0.5, QID_BIN_MAX/QID_BIN_SIZE+0.5))

    LOG.info('Plotted QID bins by items')


def plot_broad_action_by_date(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> None:
    filename = f'{PLOTPATH}broadActionByDate'

    with Plot(filename=filename, figsize=FIGSIZE_WIDE) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id']].groupby(
            by=[
                unpatrolled_changes['time'].dt.date,
                unpatrolled_changes['editsummary-magic-action-broad']
            ]
        ).count()
        tmp.unstack(level=1).plot.bar(stacked=True, grid=True, ax=ax)

        ax.legend(tmp.index.get_level_values(1).drop_duplicates().sort_values(ascending=True).tolist(), loc='best', bbox_to_anchor=(1.05, 1)) # messy, but hey ...
        ax.set_xlabel('date')
        ax.set_ylabel('number of changes')
        ax.set_xticks(range(0, 29, 7)) # also messy
        ax.set_xticklabels(plot_params['xticklabels_window'], rotation=0, ha='center')

    LOG.info('Plotted broad action by date')


def plot_broad_action_by_patrol_status(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> None:
    filename = f'{PLOTPATH}broadActionByPatrolStatus'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'], ['rc_id']].groupby(
            by=[
                unpatrolled_changes['editsummary-magic-action-broad'],
                unpatrolled_changes['rc_patrolled']
            ]
        ).count()
        tmp = tmp.merge(
            right=tmp.groupby(level=0).sum(),
            left_index=True,
            right_index=True
        )
        tmp.sort_values(by='rc_id_y', ascending=False, inplace=True)
        tmp.loc[tmp['rc_id_y']>10].unstack(level=1).plot.barh(y='rc_id_x', stacked=True, grid=True, ax=ax)

        ax.legend(['not patrolled', 'patrolled'])
        ax.invert_yaxis()
        ax.set_xlabel('number of changes')
        ax.set_ylabel('type of action')

    LOG.info('Plotted broad action by patrol status')


def plot_language_by_patrol_status(unpatrolled_changes:pd.DataFrame, termactions:list[str]) -> None: # termactions=actions['terms']
    filename = f'{PLOTPATH}languageByPatrolStatus'

    with Plot(filename=filename, figsize=FIGSIZE_TALL) as (_, ax):
        tmp = unpatrolled_changes.loc[unpatrolled_changes['editsummary-magic-action'].isin(termactions), ['rc_id']].groupby(
            by=[
                unpatrolled_changes['editsummary-magic-param1'],
                unpatrolled_changes['rc_patrolled']
            ]
        ).count()
        tmp = tmp.merge(
            right=tmp.groupby(level=0).sum(),
            left_index=True,
            right_index=True
        )
        tmp.sort_values(by='rc_id_y', ascending=False, inplace=True)
        tmp.loc[tmp['rc_id_y']>300].unstack(level=1).plot.barh(y='rc_id_x', stacked=True, grid=True, ax=ax)

        ax.legend(['not patrolled', 'patrolled'])
        ax.invert_yaxis()
        ax.set_xlabel('number of changes')
        ax.set_ylabel('language code')

    ### output to file
    tmp2 = tmp.drop(labels='rc_id_y', axis=1).unstack(fill_value=0).rename_axis((None,None), axis=1)
    tmp2.columns = [ 'unpatrolled', 'patrolled' ]
    tmp2['total'] = tmp2['unpatrolled'] + tmp2['patrolled']

    tmp2.sort_values(by=['total', 'unpatrolled'], ascending=[False, False], inplace=True)

    tmp2.to_csv('/data/project/wdpd/data/plot-language-full.tsv', sep='\t')

    LOG.info('Plotted languages by patrol status')


def plot_property_by_patrol_status(unpatrolled_changes:pd.DataFrame, claimactions:list[str]) -> None: # claimactions=actions['allclaims']
    filename = f'{PLOTPATH}propertyByPatrolStatus'

    with Plot(filename=filename, figsize=FIGSIZE_TALL) as (_, ax):
        tmp = unpatrolled_changes.loc[unpatrolled_changes['editsummary-magic-action'].isin(claimactions), ['rc_id']].groupby(
            by=[
                unpatrolled_changes['editsummary-free-property'],
                unpatrolled_changes['rc_patrolled']
            ]
        ).count()
        tmp = tmp.merge(
            right=tmp.groupby(level=0).sum(),
            left_index=True,
            right_index=True
        )
        tmp.sort_values(by='rc_id_y', ascending=False, inplace=True)
        tmp.loc[tmp['rc_id_y']>500].unstack(level=1).plot.barh(y='rc_id_x', stacked=True, grid=True, ax=ax)

        ax.legend(['not patrolled', 'patrolled'])
        ax.invert_yaxis()
        ax.set_xlabel('number of changes')
        ax.set_ylabel('property')

    ### output to file
    tmp2 = tmp.drop(labels='rc_id_y', axis=1).unstack(fill_value=0).rename_axis((None,None), axis=1)
    tmp2.columns = [ 'unpatrolled', 'patrolled' ]
    tmp2['total'] = tmp2['unpatrolled'] + tmp2['patrolled']

    query = f"""SELECT ?prop ?propertyLabel ?dtype WHERE {{
      VALUES ?property {{ wd:{" wd:".join(tmp2.index.tolist())} }}
      ?property wikibase:propertyType ?datatype .
      BIND(STRAFTER(STR(?property), 'entity/') AS ?prop) .
      BIND(STRAFTER(STR(?datatype), 'ontology#') AS ?dtype) .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language 'en' }}
    }}"""

    wdqs_data = wdqs_query(query)

    tmp2 = tmp2.merge(right=wdqs_data, left_on='editsummary-free-property', right_on='prop')
    tmp2.sort_values(by=['total', 'unpatrolled', 'propertyLabel'], ascending=[False, False, True], inplace=True)

    tmp2.to_csv('/data/project/wdpd/data/plot-property-full.tsv', sep='\t')

    LOG.info('Plotted properties by patrol status')


def plot_sitelink_by_patrol_status(unpatrolled_changes:pd.DataFrame, sitelinkactions:list[str]) -> None: # sitelinkactions=actions['sitelink'] or actions['allsitelinks']
    filename = f'{PLOTPATH}sitelinkByPatrolStatus'

    with Plot(filename=filename, figsize=FIGSIZE_TALL) as (_, ax):
        tmp = unpatrolled_changes.loc[unpatrolled_changes['editsummary-magic-action'].isin(sitelinkactions), ['rc_id']].groupby(
            by=[
                unpatrolled_changes['editsummary-magic-param1'],
                unpatrolled_changes['rc_patrolled']
            ]
        ).count()
        tmp = tmp.merge(
            right=tmp.groupby(level=0).sum(),
            left_index=True,
            right_index=True
        )
        tmp.sort_values(by='rc_id_y', ascending=False, inplace=True)
        tmp.loc[tmp['rc_id_y']>300].unstack(level=1).plot.barh(y='rc_id_x', stacked=True, grid=True, ax=ax)

        ax.legend(['not patrolled', 'patrolled'])
        ax.invert_yaxis()
        ax.set_xlabel('number of changes')
        ax.set_ylabel('project')

    ### output to file
    tmp2 = tmp.drop(labels='rc_id_y', axis=1).unstack(fill_value=0).rename_axis((None,None), axis=1)
    tmp2.columns = [ 'unpatrolled', 'patrolled' ]
    tmp2['total'] = tmp2['unpatrolled'] + tmp2['patrolled']

    tmp2.sort_values(by=['total', 'unpatrolled'], ascending=[False, False], inplace=True)

    tmp2.to_csv('/data/project/wdpd/data/plot-sitelink-full.tsv', sep='\t')

    LOG.info('Plotted sitelinks by patrol status')


def plot_other_actions_by_patrol_status(unpatrolled_changes:pd.DataFrame, otheractions:list[str]) -> None: # otheractions=actions['editentity'] + actions['linktitles'] + actions['merge'] + actions['revert'] + actions['none']
    filename = f'{PLOTPATH}otherActionsByPatrolStatus'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[unpatrolled_changes['editsummary-magic-action'].isin(otheractions), ['rc_id']].groupby(
            by=[
                unpatrolled_changes['editsummary-magic-action-broad'],
                unpatrolled_changes['rc_patrolled']
            ]
        ).count()
        tmp.unstack(level=1).plot.barh(stacked=True, grid=True, ax=ax)

        ax.legend(['not patrolled', 'patrolled'])
        ax.invert_yaxis()
        ax.set_xlabel('number of changes')
        ax.set_ylabel('type of action')

    LOG.info('Plotted other actions by patrol status')


def plot_remaining_by_date(unpatrolled_changes:pd.DataFrame, plot_params:PlotParamsDict) -> None:
    filename = f'{PLOTPATH}remainingByDate'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        tmp = unpatrolled_changes.loc[plot_params['filter_window'] & (unpatrolled_changes['rc_patrolled']==0), ['rc_id', 'actor_user', 'rc_patrolled']].groupby(
            by=unpatrolled_changes['time'].dt.date
        ).count()
        tmp['actor_anon'] = tmp['rc_id'] - tmp['actor_user']

        tmp.plot(y='rc_id', kind='line', grid=True, ax=ax)
        tmp.plot(y='actor_anon', kind='line', grid=True, ax=ax)
        tmp.plot(y='actor_user', kind='line', grid=True, ax=ax)

        ax.legend(['total still unpatrolled changes', 'by IP users', 'by registered users'])
        ax.set_xlabel('date')
        ax.set_ylabel('unpatrolled changes')
        _, _, _, ymax = ax.axis()
        ax.set(ylim=(0, ymax))
        ax.set_xticks([ mdates.date2num(ts.to_pydatetime()) for ts in plot_params['xticks_window'] ])
        ax.set_xticklabels(plot_params['xticklabels_window'])

    LOG.info('Plotted remaining workload by date')


def plot_ores_hist(unpatrolled_changes:pd.DataFrame, filt:pd.Series, grouper:pd.Series, ores_model:str, filenamepart:str, legend:Optional[list[str]]=None, titleprefix:str='') -> None:
    filename = f'{PLOTPATH}ORES-hist-{filenamepart}'

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD) as (_, ax):
        ores_notna_filter = (unpatrolled_changes['oresc_damaging'].notna()) & (unpatrolled_changes['oresc_goodfaith'].notna())
        if filt is None:
            if grouper is None:
                hist_base = unpatrolled_changes.loc[ores_notna_filter]
                cnt = len(hist_base[ores_model].index)
            else:
                hist_base = unpatrolled_changes.loc[ores_notna_filter].groupby(by=grouper)
                cnt = len(unpatrolled_changes.loc[ores_notna_filter, ores_model].index)
        else:
            if grouper is None:
                hist_base = unpatrolled_changes.loc[ores_notna_filter & filt]
                cnt = len(hist_base[ores_model].index)
            else:
                hist_base = unpatrolled_changes.loc[ores_notna_filter & filt].groupby(by=grouper)
                cnt = len(unpatrolled_changes.loc[ores_notna_filter & filt, ores_model].index)

        hist_base[ores_model].hist(bins=101, ax=ax, legend=True, alpha=0.5)

        if legend is not None:
            ax.legend(legend)

        ax.set_title(label=f'{titleprefix}n={cnt} revisions')
        ax.set_xlabel(f'ORES score for model "{ores_model[6:]}"')
        ax.set_ylabel('revisions')
        _, _, _, ymax = ax.axis()
        ax.set(xlim=(0, 1), ylim=(1, ymax))

    LOG.info('Plotted ORES histogram')


def plot_ores_hist_by_editor_type(unpatrolled_changes:pd.DataFrame) -> None:
    for ores_model in ORES_MODELS:
        plot_ores_hist(
            unpatrolled_changes,
            None,
            unpatrolled_changes['actor_user'].notna(),
            ores_model,
            f'{ores_model[6:]}AndEditorType',
            legend=['IP users', 'new registered users']
        )

        LOG.info(f'Plotted ORES histogram by editor type for model {ores_model}')


def plot_ores_hist_by_action(unpatrolled_changes:pd.DataFrame) -> None:
    for ores_model in ORES_MODELS:
        plot_ores_hist(
            unpatrolled_changes,
            unpatrolled_changes['actor_user'].notna(),
            'editsummary-magic-action-broad',
            ores_model,
            f'{ores_model[6:]}AndActionRegistered',
            titleprefix='new registered users; '
        )

        LOG.info(f'Plotted ORES histogram by action for model {ores_model} and registered users')

    for ores_model in ORES_MODELS:
        plot_ores_hist(
            unpatrolled_changes,
            unpatrolled_changes['actor_user'].isna(),
            'editsummary-magic-action-broad',
            ores_model,
            f'{ores_model[6:]}AndActionAnonymous',
            titleprefix='IP users; '
        )

        LOG.info(f'Plotted ORES histogram by action for model {ores_model} and unregistered users')


def plot_ores_hist_by_reverted(unpatrolled_changes:pd.DataFrame) -> None:
    for ores_model in ORES_MODELS:
        plot_ores_hist(
            unpatrolled_changes,
            unpatrolled_changes['actor_user'].notna(),
            unpatrolled_changes['reverted'].notna(),
            ores_model,
            f'{ores_model[6:]}AndRevertedRegistered',
            legend=['not reverted', 'reverted'],
            titleprefix='new registered users; '
        )

        LOG.info(f'Plotted ORES histogram by revert status for model {ores_model} and registered users')

    for ores_model in ORES_MODELS:
        plot_ores_hist(
            unpatrolled_changes,
            unpatrolled_changes['actor_user'].isna(),
            unpatrolled_changes['reverted'].notna(),
            ores_model,
            f'{ores_model[6:]}AndRevertedAnonymous',
            legend=['not reverted', 'reverted'],
            titleprefix='IP users; '
        )

        LOG.info(f'Plotted ORES histogram by revert status for model {ores_model} and unregistered users')


def plot_ores_hist_by_language(unpatrolled_changes:pd.DataFrame, termactions:list[str]) -> None:
    top_languages = unpatrolled_changes.loc[unpatrolled_changes['editsummary-magic-action'].isin(termactions), 'editsummary-magic-param1'].value_counts().head(10).index.to_list()

    for ores_model in ORES_MODELS:
        plot_ores_hist(
            unpatrolled_changes,
            (unpatrolled_changes['actor_user'].notna()) & (unpatrolled_changes['editsummary-magic-action'].isin(termactions)) & (unpatrolled_changes['editsummary-magic-param1'].isin(top_languages)),
            'editsummary-magic-param1',
            ores_model,
            f'{ores_model[6:]}AndLanguageRegistered',
            titleprefix='new registered users; '
        )

        LOG.info(f'Plotted ORES histogram by language for model {ores_model} and registered users')

    for ores_model in ORES_MODELS:
        plot_ores_hist(
            unpatrolled_changes,
            (unpatrolled_changes['actor_user'].isna()) & (unpatrolled_changes['editsummary-magic-action'].isin(termactions)) & (unpatrolled_changes['editsummary-magic-param1'].isin(top_languages)),
            'editsummary-magic-param1',
            ores_model,
            f'{ores_model[6:]}AndLanguageAnonymous',
            titleprefix='IP users; '
        )

        LOG.info(f'Plotted ORES histogram by language for model {ores_model} and unregistered users')


def plot_ores_hist_by_term_type(unpatrolled_changes:pd.DataFrame) -> None:
    for ores_model in ORES_MODELS:
        plot_ores_hist(
            unpatrolled_changes,
            (unpatrolled_changes['actor_user'].notna()) & (unpatrolled_changes['editsummary-magic-action-broad'].isin(['label', 'description', 'alias', 'anyterms'])),
            'editsummary-magic-action-broad',
            ores_model,
            f'{ores_model[6:]}AndTermtypeRegistered',
            titleprefix='new registered users; '
        )

        LOG.info(f'Plotted ORES histogram by term type for model {ores_model} and registered users')

    for ores_model in ORES_MODELS:
        plot_ores_hist(
            unpatrolled_changes,
            (unpatrolled_changes['actor_user'].isna()) & (unpatrolled_changes['editsummary-magic-action-broad'].isin(['label', 'description', 'alias', 'anyterms'])),
            'editsummary-magic-action-broad',
            ores_model,
            f'{ores_model[6:]}AndTermtypeAnonymous',
            titleprefix='IP users; '
        )

        LOG.info(f'Plotted ORES histogram by term type for model {ores_model} and unregistered users')


def plot_ores_heatmap(unpatrolled_changes:pd.DataFrame, filenamepart:str, filt:pd.Series, titleprefix:str='') -> None:
    filename = f'{PLOTPATH}ORES-heatmap-{filenamepart}'

    cnt = len(unpatrolled_changes.loc[filt & (unpatrolled_changes['oresc_damaging'].notna()) & (unpatrolled_changes['oresc_goodfaith'].notna())].index)

    if cnt == 0:  # quick fix to prevent script from crashing due to unavailability of anon data after introduction of temporary accounts
        return

    damaging = np_array(unpatrolled_changes.loc[filt & (unpatrolled_changes['oresc_damaging'].notna()) & (unpatrolled_changes['oresc_goodfaith'].notna()), 'oresc_damaging'])
    goodfaith = np_array(unpatrolled_changes.loc[filt & (unpatrolled_changes['oresc_damaging'].notna()) & (unpatrolled_changes['oresc_goodfaith'].notna()), 'oresc_goodfaith'])

    with Plot(filename=filename, figsize=FIGSIZE_HEATMAP) as (fig, ax):
        counts, _, _, img = ax.hist2d(
            damaging,
            goodfaith,
            norm=LogNorm(),
            bins=101,
            cmap=cm.get_cmap('coolwarm') # RdYlGn RdYlBu Spectral https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html
        )
        ax.set_title(f'{titleprefix}n={cnt} revisions')
        ax.set_xlabel('ORES score for model "damaging"')
        ax.set_ylabel('ORES score for model "goodfaith"')
        ax.set(xlim=(0, 1), ylim=(0, 1))
        cbar = fig.colorbar(img, ax=ax)
        cbar.set_label(
            f'number of revisions; max={amax(counts):.0f} revisions',
            rotation=90
        )

    LOG.info('Plotted ORES heatmap')


def plot_ores_heatmaps(unpatrolled_changes:pd.DataFrame) -> None:
    plot_ores_heatmap(
        unpatrolled_changes,
        'Registered',
        filt=(unpatrolled_changes['actor_user'].notna()),
        titleprefix='new registered users; '
    )
    LOG.info('Plotted ORES heatmap for registered users')

    plot_ores_heatmap(
        unpatrolled_changes,
        'Anonymous',
        filt=(unpatrolled_changes['actor_user'].isna()),
        titleprefix='IP users; '
    )
    LOG.info('Plotted ORES heatmap for unregistered users')


def get_bins(maximum:int) -> range:
    if maximum <= 24:
        return range(0, maximum+1, 1)
    elif maximum <= 48:
        return range(0, maximum+1, 2)
    elif maximum <= 96:
        return range(0, maximum+1, 6)
    elif maximum <= 168:
        return range(0, maximum+1, 12)
    elif maximum <= 336:
        return range(0, maximum+1, 24)
    else:
        return range(0, maximum+1, 48)


def get_xticks(maximum:int) -> int:
    if maximum <= 6:
        return 1
    elif maximum <= 12:
        return 2
    elif maximum <= 24:
        return 3
    elif maximum <= 48:
        return 4
    elif maximum <= 96:
        return 6
    elif maximum <= 192:
        return 12
    elif maximum <= 384:
        return 24
    else:
        return 48


def make_patrol_progress_plot(patrol_progress:pd.DataFrame, language:str) -> None:
    filename = f'{PLOTPATH}progress_by_lang/patrol-progress_{language}'

    filt = (patrol_progress['editsummary-magic-param1']==language)

    max_patrol_time = m_ceil(patrol_progress.loc[filt, 'patrol_delay_seconds'].max().total_seconds() / 3600)
    ticks = get_xticks(max_patrol_time)
    bins = get_bins(max_patrol_time)

    hours = patrol_progress.loc[filt, 'patrol_delay_seconds'].dt.total_seconds() / 3600

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD, svg=False) as (_, ax):
        try:
            hours.hist(bins=bins, ax=ax)
        except ValueError:
            pass
        else:
            ax.legend([ language ])
            ax.set_xlabel('patrol delay (hours)')
            ax.set_ylabel('number of revisions')
            _, _, _, ymax = ax.axis()
            ax.set_xticks(range(0, (m_ceil(max_patrol_time / ticks) + 1) * ticks, ticks))
            ax.set(xlim=(0, m_ceil(max_patrol_time / ticks) * ticks), ylim=(0, ymax))

    LOG.info(f'Plotted patrol progress plot for language "{language}"')


def make_patrol_progress_percentiles(patrol_progress:pd.DataFrame, language:str) -> None:
    filename = f'{PLOTPATH}progress_by_lang/patrol-progress-percentiles_{language}'

    filt = patrol_progress['editsummary-magic-param1']==language

    tmp = patrol_progress.loc[filt, 'patrol_delay_seconds'].dt.total_seconds() / 3600
    percentiles = range(0, 101)
    values = []
    for i in percentiles:
        values.append(tmp.quantile(i/100))

    max_patrol_time = m_ceil(patrol_progress.loc[filt, 'patrol_delay_seconds'].max().total_seconds() / 3600)
    ticks = get_xticks(max_patrol_time)

    with Plot(filename=filename, figsize=FIGSIZE_STANDARD, svg=False) as (_, ax):
        ax.plot(values, percentiles, '+')
        ax.legend([ language ])
        ax.set_xlabel('patrol delay (hours)')
        ax.set_ylabel('fraction of patrolled revisions (%)')
        #xmin, xmax, ymin, ymax = ax.axis()
        ax.set_xticks(range(0, (m_ceil(max_patrol_time / ticks) + 1) * ticks, ticks))
        ax.set(xlim=(0, m_ceil(max_patrol_time / ticks) * ticks), ylim=(0, 100))
        ax.grid(True)

    LOG.info(f'Plotted patrol progress percentiles plot for language "{language}"')


def make_all_patrol_progress_stats(patrol_progress:pd.DataFrame) -> None:
#   languages = list(patrol_progress['editsummary-magic-param1'].unique())
    languages = [ 'de', 'en', 'fr', 'es', 'it', 'ru' ]

    existing_plots = glob(f'{PLOTPATH}progress_by_lang/patrol-progress_*.png')
    for existing_plot in existing_plots:
        lang = existing_plot[58:-4].replace('_', ' ')
        if lang not in languages: # hacky
            delete_file(existing_plot)
            delete_file(existing_plot.replace('/patrol-progress_', '/patrol-progress-percentiles_'))

    for language in languages:
        make_patrol_progress_plot(patrol_progress, language)
        make_patrol_progress_percentiles(patrol_progress, language)
