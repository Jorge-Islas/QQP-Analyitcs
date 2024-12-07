from glob import glob
import pandas as pd

def process_puebla_info(path):
    df = pd.read_csv(
        path,
        names=[
            "producto",
            "presentacion",
            "marca",
            "categoria",
            "catalogo",
            "precio",
            "fechaRegistro",
            "cadenaComercial",
            "giro",
            "nombreComercial",
            "direccion",
            "estado",
            "municipio",
            "latitud",
            "longitud"
        ]
    )
    df = df[df["estado"]=="PUEBLA"]
    df.to_csv(new_file_path,index=False)

if len(glob("data\\puebla\\*.csv")) == 0:
    data_dirs = glob("data\\QQP*")

    for dir_path in data_dirs:
        # Get subdirectory
        sub_dir = dir_path.split("_")[-1]
        
        # Filter info by Puebla state
        print(f"Processing {sub_dir} info...")
        for path in glob(f"{dir_path}\\{sub_dir}\\*.csv"):
            file_name = path.split("\\")[-1].split(".")[0]

            new_file_path = "data\\puebla\\"+file_name+".csv"

            process_puebla_info(path)

            print("File "+new_file_path+" saved successfully.")

        print(f"{sub_dir} info was processed successfully.\n")

    print("\nPuebla info saved successfully.")

# Get Puebla info
df_list = []
for path in glob("data\\puebla\\*.csv"):
    df_list.append(pd.read_csv(path))
print(len(df_list))

# Remove accents and punctuation
print("\nRemoving accents and punctuation...")

df = pd.concat(df_list).convert_dtypes()

string_cols = df.select_dtypes("string[python]").columns

for col in string_cols:
    df[col] = df[col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df[col] = df[col].str.replace('[^0-9a-zA-Z ]+', "", regex=True)

print("\nAccents removed successfully.")

# Save final data
df.to_csv("data\\datos_puebla.csv", index=False)

print("\nData saved successfully.")