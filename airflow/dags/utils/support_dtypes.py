import polars as pl
def set_dtypes(dtype_aux):
    dtypes = {}
    for key,value in dtype_aux.items():
        dtypes.update({key:eval(value)})
    return dtypes