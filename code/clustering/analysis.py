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
    return cmap(colors_i)


top_words = 5
ext_docs = 10
workdir = Path('results/result_66')
records, words, matrix = np.load('./results/result_cloud/vec_doc.npy')
with (workdir / 'vars.pickle').open('rb') as f:
    vars_ = pickle.load(f)
# orig_matrix tfidf lsa k_means


lengths = matrix.sum(axis=1)
max_length = lengths.max()


word_count = matrix.sum(axis=0)
max_words = word_count.argsort()[::-1]


hover = HoverTool(tooltips=[
    ('Year', '@year'),
    ('Quarter', '@qtr'),
    ('CIK', '@cik'),
    ('Length', '@length'),
    ('Rel. freq.', '@rel_freq'),
    ('Count', '@count'),
])
tools = "pan,box_zoom,reset,wheel_zoom".split(',') + [hover]
freq_graph = figure(tools=tools)
slider = Slider(start=0, end=max_length, value=1, step=10,
                title="Length cutoff")


serieses = []
for i in range(top_words):
    series = ColumnDataSource(dict(i=[], rel_length=[],
                                   rel_freq=[], length=[],
                                   year=[], qtr=[], cik=[], count=[], word=[], color=[]))
    serieses.append(series)
    # p.circle(i, length / max_length * rel_freq[0], radius=2)
    # p.line(i, rel_freq)


# doc_count = matrix.sum(axis=1)
# mask = doc_count < 5e3
# global matrix
# matrix = np.delete(matrix, np.arange(len(matrix))[mask], axis=0)
# records = [record for record, c in zip(records, mask) if not c]
def filter(array):
    return [array[i] for i in range(len(array)) if lengths[i] >= slider.value]

def threshold(attr, old, new):
    print('j')
    for series, i, color in zip(serieses, max_words[:top_words], n_colors(top_words)):
        word = words[i]
        rel_freq = matrix[:, i] / lengths
        doc_idxs = rel_freq.argsort()
        print(new)
        doc_idxs = filter(doc_idxs)
        rel_freq = rel_freq[doc_idxs]
        i = filter(np.arange(len(records)))
        length = lengths[doc_idxs]
        count = rel_freq[doc_idxs] * length
        rel_length = (lengths[doc_idxs] / max_length)
        year = [records[doc_idx][0] for doc_idx in doc_idxs]
        qtr = [records[doc_idx][1] for doc_idx in doc_idxs]
        cik = [records[doc_idx][2] for doc_idx in doc_idxs]
        data = dict(rel_freq=rel_freq, i=i, length=length, count=count, rel_length=rel_length, year=year, qtr=qtr, cik=cik, word=[word for j in i], color=[color for j in i])
        series.data = data


threshold(None, None, 0)
for series in serieses:
    freq_graph.circle('i', 'rel_freq', radius=0.5, alpha='rel_length',
                      source=series, line_width=0, legend='word', color='color')


length_graph = figure(tools=tools[:-1])
i = np.arange(len(records))
length_graph.line(i, lengths)


freq_graph.legend.location = "top_left"
freq_graph.legend.click_policy = "hide"

# slider.on_change(threshold)
show(row(freq_graph, column(length_graph, slider)))
