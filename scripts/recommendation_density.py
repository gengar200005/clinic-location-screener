"""Spatial reference weights; original candidate rows are never removed."""
import numpy as np
import pandas as pd


def feature_columns(frame):
    return sorted(c for c in frame if c.split('_')[0] in ['demand','internal','endo','hospital','gastric'] and c.split('_')[-1] in ['1000','1500'] and not c.startswith('demand_upper'))


def reference_weights(frame, spec):
    if spec['mode'] == 'legacy_coordinates':
        return None
    if spec['mode'] == 'uniform_midpoint':
        return pd.Series(1.,index=frame.index)
    f = frame.copy()
    cols = feature_columns(f)
    # A reported building identity anchors copied-source coordinates to its canonical
    # source row. No distance clustering, no merging across rail/road catchments.
    f['_canonical'] = 1
    if 'building_key' in f and 'building_identity_state' in f:
        trusted = f.building_identity_state.eq('reported_building_id') & f.building_key.notna()
        if trusted.any():
            f.loc[trusted, '_canonical'] = (~f.loc[trusted, 'anchor_id'].eq('commercial_' + f.loc[trusted,'building_key'].astype(str))).astype(int)
            positions = f[trusted].sort_values(['_canonical','anchor_id']).drop_duplicates('building_key').set_index('building_key')[['x','y']]
            for axis in ['x','y']:
                f.loc[trusted, axis] = f.loc[trusted,'building_key'].map(positions[axis])
    size = spec['cell_size_m']; ox, oy = spec['offset_m']
    f['_cx'] = np.floor((f.x - ox) / size).astype(int)
    f['_cy'] = np.floor((f.y - oy) / size).astype(int)
    # Exact complete evidence signatures share reference mass within a cell.
    # Distinct buildings stay in the target inventory, even if their values coincide.
    keys = ['_cx','_cy'] + cols
    group = f.groupby(keys, dropna=False, sort=False)
    duplicate_count = group.anchor_id.transform('size')
    signatures = f.drop_duplicates(keys).groupby(['_cx','_cy']).size()
    cell_count = pd.MultiIndex.from_frame(f[['_cx','_cy']]).map(signatures)
    return pd.Series(1 / (duplicate_count.to_numpy() * np.asarray(cell_count)), index=f.index)


def percentile(values, weights=None):
    if weights is None:
        return values.rank(pct=True) * 100
    # Weighted midpoint empirical CDF. Deliberately differs from legacy rank/N
    # by 50/N on equal-weight rows: this convention is recorded in the report.
    mass = pd.DataFrame({'value': values, 'weight': weights}).groupby('value').weight.sum().sort_index()
    mapped = 100 * (mass.cumsum() - mass / 2) / mass.sum()
    return values.map(mapped)


def density_summary(frame, spec):
    size = spec['cell_size_m']; ox, oy = spec['offset_m']
    f=frame.assign(cell_x=np.floor((frame.x-ox)/size).astype(int),cell_y=np.floor((frame.y-oy)/size).astype(int))
    f['cell']=list(zip(f.cell_x,f.cell_y))
    f['reported_building']=f.building_key.where(f.building_identity_state.eq('reported_building_id'))
    return f.groupby(['region','admin_code','population_name'],sort=True).agg(coordinates=('anchor_id','size'),
        reported_buildings=('reported_building','nunique'),occupied_cells=('cell','nunique')).reset_index()
