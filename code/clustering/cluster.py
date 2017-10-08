import pickle
import heapq
import numpy as np
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import KMeans
from sklearn.feature_selection import VarianceThreshold
from util.timer import timer
from util.new_directory import new_directory
from util.project import project

class FeatureSelector(object):
    def __init__(self, selector=None):
        pass


    def fit_transform(self, matrix):
        pass
        # matrix_ = tfidf.fit_transform(matrix).toarray()

        # to_remove = int(matrix_.shape[1] * stop_words)
        # std = VarianceThreshold().fit(matrix_).variances_
        # std.sort()
        # threshold = std[to_remove]
        # print('Removing', to_remove, 'stop words of', std.shape[0], '; Thresholding at', threshold)
        # selector.threshold = threshold
        # old_shape = matrix.shape
        # matrix = selector.fit_transform(matrix)
        # new_shape = matrix.shape
        # print('{old_shape} -> {new_shape}'.format(**locals()))


def select_features(words, matrix, dims=600, stop_words=0.8, clusters=20, keywords=30):
    tfidf = TfidfTransformer(norm='l2')
    lsa = TruncatedSVD(dims, n_iter=3)
    selector = FeatureSelector()
    k_means = KMeans(n_clusters=clusters, init='k-means++', max_iter=100,
                     n_init=10)

    count = matrix.sum(axis=0)
    # with timer('pre idf'):
    #     matrix = selector.fit_transform(matrix)

    with timer('idf'):
        matrix = tfidf.fit_transform(matrix)

    with timer('lsa'):
        old_shape = matrix.shape
        matrix = lsa.fit_transform(matrix)
        new_shape = matrix.shape
        exp_var = lsa.explained_variance_ratio_.sum() * 100
        print("Explained variance of the SVD step: {exp_var:1f}% ({old_shape} -> {new_shape})".format(**locals()))

	# TODO: crossval cluster number
    with timer('kmeans'):
        k_means.fit(matrix)

    for cluster_center in k_means.cluster_centers_:
        cluster_center = cluster_center[np.newaxis, :]
        word_vec = lsa.inverse_transform(cluster_center)
        for (v, keyword) in heapq.nlargest(keywords, zip(list(word_vec[0]), words)):
            print(keyword, end=' ')
        print()

    dir_ = new_directory()
    print(dir_)
    save = 'orig_matrix tfidf lsa k_means'.split(' ')
    with (dir_ / 'vars.pickle').open('wb') as f:
        pickle.dump(project(locals(), save), f)

def graph():
    import matplotlib
    matplotlib.use('qt5agg')
    import matplotlib.pyplot as plt
    plt.figure()
    plt.plot(std)
    plt.show()

if __name__ == '__main__':
    records, words, matrix = np.load('./results/result_cloud/vec_doc.npy')
    print(len(records), len(words), matrix.shape)
    select_features(words, matrix)