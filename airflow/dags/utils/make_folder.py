# Get href values for download func
#########################
####SUPPORT FUNCTIONS####
#########################
def _make_folder(path) -> None:
    import os
    
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        pass