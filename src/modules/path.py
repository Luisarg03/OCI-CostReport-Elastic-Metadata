#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil


def create_paths():
    '''
    Renueva el directorio donde se descargan los files.
    Parameters
    ----------
    
    Returns
    ---------
    str
    '''
    folder = '../Metadata/'
    try:
        shutil.rmtree(folder)
    except:
        pass
    if not os.path.exists(folder):
        os.makedirs(folder)

    return folder