from bokeh.io import curdoc
from bokeh.plotting import figure, show, ColumnDataSource
from bokeh.layouts import row, column
from bokeh.models import HoverTool
from bokeh.models.widgets import Slider
import matplotlib.cm as cm
import numpy as np
import pickle
from pathlib import Path


def n_colors(N, cmap='viridis'):
    """Return a discrete colormap from the continuous colormap cmap.

        cmap: colormap instance, eg. cm.jet.
        N: number of colors.

    Example
        x = resize(arange(100), (5,100))
        djet = cmap_discretize(cm.jet, 5)
        imshow(x, cmap=djet)

    Source:
http://scipy-cookbook.readthedocs.io/items/Matplotlib_ColormapTransformations.html
    """

    if type(cmap) == str:
        cmap = cm.get_cmap(cmap)
    colors_i = np.concatenate((np.linspace(0, 1., N), (0., 0., 0., 0.)))
    return ['#{:02x}{:02x}{:02x}'.format(*map(int, rgba[:3]*256)) for rgba in cmap(colors_i)]


top_words = 3
random_words = 200
skip = 300
ext_docs = 10
workdir = Path('results/result_66')
records, words, matrix = np.load('./results/result_cloud/vec_doc.npy')
with (workdir / 'vars.pickle').open('rb') as f:
    vars_ = pickle.load(f)
# orig_matrix tfidf lsa k_means


lengths = matrix.sum(axis=1)
max_length = lengths.max()


word_freq = (matrix / lengths[:, np.newaxis]).mean(axis=0)
max_words = word_freq.argsort()[::-1]


slider = Slider(start=0, end=max_length, value=0, step=10,
                title="Length cutoff")

serieses = []
for i in range(top_words*2):
    series = ColumnDataSource()
    serieses.append(series)
    # p.circle(i, length / max_length * rel_freq[0], radius=2)
    # p.line(i, rel_freq)


# doc_count = matrix.sum(axis=1)
# mask = doc_count < 5e3
# global matrix
# matrix = np.delete(matrix, np.arange(len(matrix))[mask], axis=0)
# records = [record for record, c in zip(records, mask) if not c]
def filter(array):
    return array[lengths >= slider.value]


bad_data = ColumnDataSource()
good_data = ColumnDataSource()


def threshold(attr, old, new):
    for series, i, color in zip(serieses, np.concatenate((max_words[:top_words], max_words[random_words:random_words+skip*top_words:skip])), n_colors(top_words*2)):
        word = words[i]
        rel_freq = matrix[:, i] / lengths
        doc_idxs = rel_freq.argsort()
        doc_idxs = filter(doc_idxs)
        rel_freq = rel_freq[doc_idxs]
        i = filter(np.arange(len(records)))
        length = lengths[doc_idxs]
        count = rel_freq * length
        rel_length = (lengths[doc_idxs] / max_length)
        year = [records[doc_idx][0] for doc_idx in doc_idxs]
        qtr = [records[doc_idx][1] for doc_idx in doc_idxs]
        cik = [records[doc_idx][2] for doc_idx in doc_idxs]
        data = dict(rel_freq=rel_freq, word=[word for j in i], color=[color for j in i], count=count, i=i,
                    length=length, rel_length=rel_length, year=year, qtr=qtr, cik=cik)
        series.data = data

    sort = np.argsort(lengths)
    mask = lengths[sort] < slider.value
    bad_i = np.arange(len(lengths))[mask]
    good_i = np.arange(len(lengths))[~mask]
    rel_length = (lengths[sort] / max_length)**(1/5)
    year = np.array([records[doc_idx][0] for doc_idx in sort])
    qtr = np.array([records[doc_idx][1] for doc_idx in sort])
    cik = np.array([records[doc_idx][2] for doc_idx in sort])
    bad_data.data = dict(i=bad_i, length=lengths[sort][mask], rel_length=rel_length[mask], year=year[mask], qtr=qtr[mask], cik=cik[mask])
    good_data.data = dict(i=good_i, length=lengths[sort][~mask], rel_length=rel_length[~mask], year=year[~mask], qtr=qtr[~mask], cik=cik[~mask])


threshold(None, None, None)


hover1 = HoverTool(tooltips=[
    ('Word', '@word'),
    ('Year', '@year'),
    ('Quarter', '@qtr'),
    ('CIK', '@cik'),
    ('Length', '@length'),
    ('Rel. freq.', '@rel_freq'),
    ('Count', '@count'),
])
hover2 = HoverTool(tooltips=[
    ('Word', '@year'),
    ('Year', '@year'),
    ('Quarter', '@qtr'),
    ('CIK', '@cik'),
    ('Length', '@length'),
])
tools = "pan,box_zoom,reset,wheel_zoom".split(',')
freq_graph = figure(tools=tools + [hover1], x_range=(0, len(matrix)), y_range=(0, word_freq[max_words[0]].max()))

for series in serieses:
    freq_graph.circle('i', 'rel_freq', radius=0.5, fill_alpha=1,
                      line_alpha=0,
                      source=series, line_width=0,
                      legend=series.data['word'][0], color='color')


length_graph = figure(tools=tools + [hover2])
length_graph.line('i', 'length', color='red', source=bad_data)
length_graph.line('i', 'length', color='blue', source=good_data)


freq_graph.legend.location = "top_left"
freq_graph.legend.click_policy = "hide"

slider.on_change('value', threshold)
root = row(freq_graph, column(length_graph, slider))
#show(root)
curdoc().add_root(root)
