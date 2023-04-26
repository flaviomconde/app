#importamos las librerias necesarias

import numpy as np

from tkinter import *
import pandas as pd
from tkinter import filedialog

root = Tk()
root.geometry("350x300")
root.title("ETL - Contabillium")
root.configure(bg='#333333')
frame = Frame(bg='#333333')

global archivo
archivo = False

def leer_archivos():
	# Elegimos el archivo deseado
	open_file = filedialog.askopenfilename(title='Abrir archivo', filetypes=(('Archivos de excel', '*.xlsx'),))
	
	# Chequeamos que se halla elegido un archivo
	if open_file:
		# Hacemos global al archivo, para poder acceder luego
		global archivo
		archivo = open_file
def procesar_registros():
    if archivo:
    #traemos las base de datos
        df1 = pd.read_excel(archivo,sheet_name=0)
        df2 = pd.read_excel(archivo,sheet_name=1)
        df3 = pd.read_excel(archivo,sheet_name=2)

        #pasamos los nombres de la columnas a minusculas
        df_arg1 =  df1.rename(columns=str.lower)
        df_uru2 =  df2.rename(columns=str.lower)
        df_chi3 =  df3.rename(columns=str.lower)

        #eliminamos la columna no necesarias
        df_arg = df_arg1.drop(df_arg1.iloc[:, 5:13], axis=1)
        df_uru = df_uru2.drop(df_uru2.iloc[:, 5:13], axis=1)
        df_chi = df_chi3.drop(df_chi3.iloc[:, 5:13], axis=1)

        #eliminamos la columna segment
        df_ar = df_arg.drop('segment', axis=1) 
        df_ur = df_uru.drop('segment', axis=1) 
        df_ch = df_chi.drop('segment', axis=1) 

        #agregamos la columna pais con el pais correspondiente para cada registro
        df_ar['pais'] = 'Argentina'
        df_ur['pais'] = 'Uruguay'
        df_ch['pais'] = 'Chile'

        #concatenamos los 3 registros
        df_auc = pd.concat([df_ar, df_ur,df_ch], axis=0)

        # filtramos la columna origen crudo para trabajarla
        df_a_crud = df_ar['origen crudo'].str.replace('undefined\|', '')
        df_u_crud = df_ur['origen crudo'].str.replace('undefined\|', '')
        df_c_crud = df_ch['origen crudo'].str.replace('undefined\|', '')

        #concatenamos los registros de origen crudo
        df_Total = pd.concat([df_a_crud, df_u_crud,df_c_crud], axis=0)

        #separamos la columna por su primer separador que es '|'
        df_1 = df_Total.str.split('|', expand=True)

        #separamos la columna por su segundo separador '-' y le asignamos los nombres
        df_1[['tipo de anuncio', 'pais1', 'adset', 'mes del anuncio']] = df_1[4].str.split('-', expand=True)

        # le asignamos nombre a las columnas que faltan
        df = df_1.rename(columns={0: 'source', 1: 'canal', 2: 'objetivo',3: 'audiencia', 4: 'crudo'}) 

        #eliminamos las columnas no necesarias
        dft1 = df.drop('crudo', axis=1)  
        dft = dft1.drop('pais1', axis=1)  

        # Verificar si la columna contiene la palabra "google"
        mask = dft['source'].str.contains('google')

        # Reemplazar la columna con "google" si cumple la condición
        dft['source'][mask] = 'google'

        # agregamos la nueva columna segment con la condición especificada
        dft['segment'] = ['POS' if 'pos' in val else 'ERP' for val in df['tipo de anuncio']]

        #concatenamos los registros inicial y separados
        dffinal = pd.concat([df_auc, dft], axis=1)

        # eliminamos la columan origen crudo
        dff = dffinal.drop('origen crudo', axis=1) 

        #guardamos en csv el registro final
        dff.to_csv('registro_desconcat1.csv',index=False)

# Creamos los widgets
label = Label(frame, text = "ETL", font=('Arial', 25), bg='#333333', fg='#FF2569')
label_open = Label(frame, text = "Abrir archivo:", font=('Arial', 14), bg='#333333', fg='#FF2569')
open_button = Button(frame, text="Open", bg="#00C7AF", fg="white", height="2", width="10", relief=RAISED, 
		    borderwidth=3, font=('Arial', 14), command=leer_archivos)
label_excecute = Label(frame, text = "Ejecutar:", font=('Arial', 14), bg='#333333', fg='#FF2569')
excecute_button = Button(frame, text = "Run",bg="#00C7AF", fg="white", height="2", width="10", relief=RAISED, 
			borderwidth=3, font=('Arial', 14), command = procesar_registros)


# Posicionamos los widgets
label.grid(row=0, column=0, columnspan=2, sticky='news', pady=25)
label_open.grid(row=1, column=0, padx=10)
open_button.grid(row=1, column=1, padx= 2, pady=10)
label_excecute.grid(row=2, column=0, padx=10)
excecute_button.grid(row=2, column=1, pady=10)
frame.pack()

root.mainloop()