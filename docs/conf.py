#!/usr/bin/env python3

import sys
import os
import shlex

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
]

templates_path = ['_templates']

source_suffix = '.rst'

master_doc = 'index'

project = 'aiorq'
copyright = '2015, Artem Malyshev'
author = 'Artem Malyshev'

version = '0.2'
release = '0.2'

language = None

exclude_patterns = ['_build']

pygments_style = 'sphinx'

todo_include_todos = False

html_theme = 'alabaster'

html_static_path = ['_static']

htmlhelp_basename = 'aiorqdoc'

latex_elements = {}

latex_documents = [
    (master_doc, 'aiorq.tex', 'aiorq Documentation',
     'Artem Malyshev', 'manual'),
]

man_pages = [
    (master_doc, 'aiorq', 'aiorq Documentation',
     [author], 1)
]

texinfo_documents = [
    (master_doc, 'aiorq', 'aiorq Documentation',
     author, 'aiorq', 'One line description of project.',
     'Miscellaneous'),
]
