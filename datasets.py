from lda.utils import ldac2dtm


def load_ldac(file_path):
    return ldac2dtm(open(file_path), offset=0)


def load_vocab(file_path):
    with open(file_path) as f:
        vocab = tuple(f.read().split())
    return vocab


def load_titles(file_path):
    with open(file_path) as f:
        titles = tuple(line.strip() for line in f.readlines())
    return titles
