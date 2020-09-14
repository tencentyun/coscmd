XCOSCMD

发布指南
__________

::

    python setup.py sdist bdist_wheel
    pip install twine
    twine check dist/*
    twine upload dist/*

