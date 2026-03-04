import zipfile

with zipfile.ZipFile(r"C:\Users\andric\Desktop\Progetto.egp", 'r') as zip_ref:
    zip_ref.extractall("estratto2/")
