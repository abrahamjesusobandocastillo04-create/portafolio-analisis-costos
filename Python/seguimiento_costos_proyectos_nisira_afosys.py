# ============================================================
# AUTOMATIZACIÓN DE SEGUIMIENTO DE COSTOS POR PROYECTO
# Fuente: NISIRA + AFOSYS
# Autor: Abraham Obando
# ============================================================

# IMPORTACIÓN DE LIBRERÍAS
import pandas as pd

# ============================================================
# FUNCIÓN DE LIMPIEZA GENERAL DE DATAFRAMES
# - Normaliza nombres de columnas
# - Elimina espacios innecesarios
# ============================================================
def limpiar_df(tabla):
    tabla.columns = tabla.columns.str.strip()
    tabla.columns = tabla.columns.str.replace(r'\s+', '_', regex=True)

    cols_texto = tabla.select_dtypes(include='object').columns
    tabla[cols_texto] = tabla[cols_texto].apply(lambda c: c.str.strip())

    return tabla

# ============================================================
# CARGA DE ARCHIVOS HISTÓRICOS (OC, OS, CXP Y PROYECTOS)
# ============================================================
oc_2023 = limpiar_df(pd.read_excel(r"D:\Users\AX.COSTOS3\Desktop\ABRAHAM\MANTENIMIENTO\OC_2023.xlsx"))
oc_2024 = limpiar_df(pd.read_excel(r"D:\Users\AX.COSTOS3\Desktop\ABRAHAM\MANTENIMIENTO\OC_2024.xlsx"))
oc_2025 = limpiar_df(pd.read_excel(r"D:\Users\AX.COSTOS3\Desktop\ABRAHAM\MANTENIMIENTO\OC_2025_2026.xlsx"))

os_2023 = limpiar_df(pd.read_excel(r"D:\Users\AX.COSTOS3\Desktop\ABRAHAM\MANTENIMIENTO\OSR_2023.xlsx"))
os_2024 = limpiar_df(pd.read_excel(r"D:\Users\AX.COSTOS3\Desktop\ABRAHAM\MANTENIMIENTO\OSR_2024.xlsx"))
os_2025 = limpiar_df(pd.read_excel(r"D:\Users\AX.COSTOS3\Desktop\ABRAHAM\MANTENIMIENTO\OSR_2025_2026.xlsx"))

cxp = limpiar_df(pd.read_excel(r"D:\Users\AX.COSTOS3\Desktop\ABRAHAM\MANTENIMIENTO\CXP.xlsx"))
proyectos_afosys = limpiar_df(pd.read_excel(r"D:\Users\AX.COSTOS3\Desktop\ABRAHAM\MANTENIMIENTO\PROYECTOS_AFOSYS.xlsx",header=1))

# ============================================================
# NORMALIZACIÓN DE TIPOS DE DATOS Y COLUMNAS CLAVE
# ============================================================
for df in [oc_2023, oc_2024, oc_2025, os_2023, os_2024, os_2025]:
    df['IDPROYECTO'] = (
        pd.to_numeric(df['IDPROYECTO'], errors='coerce')
        .astype('Int64')
    )

proyectos_afosys["ID_PROYECTO"] = pd.to_numeric(proyectos_afosys["ID_PROYECTO"],errors = "coerce")
proyectos_afosys["EJECUTADO"] = (proyectos_afosys["EJECUTADO"].str.replace(r"[$,]", "", regex=True).astype(float))
proyectos_afosys = proyectos_afosys.rename(columns={"ID_PROYECTO" : "IDPROYECTO_AFO", "EJECUTADO" : "EJECUTADO_AFO"})
df_proyectos = proyectos_afosys[["IDPROYECTO_AFO","EJECUTADO_AFO"]].sort_values("IDPROYECTO_AFO").reset_index(drop = True)
print("✔️ Datasets cargados y limpiados correctamente.")

# ============================================================
# DEFINICIÓN DE CRITERIOS Y FILTROS DE NEGOCIO
# ============================================================
estados_oc = ['Atendido Total', 'Atendido Parcial', 'Aprobado']
estados_os = ['Aprobado', 'Conformidad']

metodo_pago = 'CONTADO'
estado_cxp  = 'PAGADA'
destino     = 'CONTADO'
print("🔍 Aplicando reglas de negocio y filtros operativos...")

# ============================================================
# CONSOLIDACIÓN DE PERIODOS 2024–2025
# ============================================================
oc_2425 = pd.concat([oc_2024, oc_2025], ignore_index=True)
os_2425 = pd.concat([os_2024, os_2025], ignore_index=True)

# ============================================================
# OBTENCIÓN DEL LISTADO ÚNICO DE PROYECTOS
# ============================================================
proyectos = pd.concat([
    oc_2023['IDPROYECTO'],
    oc_2425['IDPROYECTO'],
    os_2023['IDPROYECTO'],
    os_2425['IDPROYECTO']
]).dropna().unique()

print("🚀 Iniciando cálculo de costos por proyecto...")


# ============================================================
# FUNCIÓN DE CÁLCULO DE COSTO TOTAL POR PROYECTO
# ============================================================
def calcular_total_proyecto(id_proyecto):

    # --- AÑO 2023 (SIN FILTRO DE MÉTODO DE PAGO) ---
    oc_2023_total = oc_2023[
        (oc_2023['IDPROYECTO'] == id_proyecto) &
        (oc_2023['ESTADO'].isin(estados_oc)) &
        (oc_2023["AREA"].str.contains("PROYECTOS", case=False, na=False))
    ]['SUBTOTALMEX'].sum()

    os_2023_total = os_2023[
        (os_2023['IDPROYECTO'] == id_proyecto) &
        (os_2023['ESTADO'].isin(estados_os))
    ]['SUBTOTALMEX'].sum()

    # --- AÑOS 2024–2025 (PAGOS CONTADO) ---
    oc_2425_total = oc_2425[
        (oc_2425['IDPROYECTO'] == id_proyecto) &
        (oc_2425['ESTADO'].isin(estados_oc)) &
        (oc_2425['DSC_FPAGO'] == metodo_pago) &
        (oc_2425["AREA"].str.contains("PROYECTOS", case=False, na=False))
    ]['SUBTOTALMEX'].sum()

    os_2425_total = os_2425[
        (os_2425['IDPROYECTO'] == id_proyecto) &
        (os_2425['ESTADO'].isin(estados_os)) &
        (os_2425['DSC_FPAGO'] == metodo_pago)
    ]['SUBTOTALMEX'].sum()

    # --- AÑOS 2024–2025 (PAGOS NO CONTADO → CXP) ---
    ids_no_contado = pd.concat([
        oc_2425[
            (oc_2425['IDPROYECTO'] == id_proyecto) &
            (oc_2425['DSC_FPAGO'] != metodo_pago) &
            (oc_2425["AREA"].str.contains("PROYECTOS", case=False, na=False))
        ][['IDCOMPRA']].rename(columns={'IDCOMPRA': 'ID_ORIGEN'}),

        os_2425[
            (os_2425['IDPROYECTO'] == id_proyecto) &
            (os_2425['DSC_FPAGO'] != metodo_pago) 
        ][['IDSERVICIO']].rename(columns={'IDSERVICIO': 'ID_ORIGEN'})
    ])

    total_cxp = cxp[
        (cxp['ID_ORIGEN'].isin(ids_no_contado['ID_ORIGEN'])) &
        (cxp['ESTADO'] == estado_cxp) &
        (cxp['DESTINO'] == destino) & 
        (cxp["ESTADO_REGISTRO"] == 1)
    ]['MONTO_DOLARES'].sum()

    # --- TOTAL CONSOLIDADO ---
    total_final = (
        oc_2023_total +
        os_2023_total +
        oc_2425_total +
        os_2425_total +
        total_cxp
    )

    return total_final

# ============================================================
# EJECUCIÓN DEL CÁLCULO PARA TODOS LOS PROYECTOS
# ============================================================
resultados = []

for p in proyectos:
    resultados.append({
        'IDPROYECTO_NISIRA': p,
        'EJECUTADO_NISIRA': calcular_total_proyecto(p)
    })

df_resultado = pd.DataFrame(resultados).sort_values('IDPROYECTO_NISIRA').reset_index(drop=True)


# ============================================================
# COMPARATIVO FINAL NISIRA vs AFOSYS
# ============================================================
df_final = df_resultado.merge(
    df_proyectos,
    left_on="IDPROYECTO_NISIRA",
    right_on="IDPROYECTO_AFO",
    how = "outer"
)
df_final.to_excel(r"D:\Users\AX.COSTOS3\Desktop\ABRAHAM\MANTENIMIENTO\Resultados\COMPARATIVO_PROYECTOS.xlsx",index=False)
print("✅ Comparativo de proyectos exportado correctamente.")
