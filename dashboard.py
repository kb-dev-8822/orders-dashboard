import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection
import re

# ==========================================
# ⚙️ הגדרות שמות עמודות (מותאם למה שביקשת)
# ==========================================
COL_SKU = 'מקט'
COL_CUSTOMER = 'שם פרטי'
COL_PHONE = 'טלפון'
COL_ORDER_NUM = 'מספר הזמנה' # עמודה חדשה לחיפוש
COL_QUANTITY = 'כמות'        # עמודה לסיכום חבילות
COL_DATE = 'תאריך'           # (לא שינינו)
COL_SHIP_NUM = 'מספר משלוח'  # (לא שינינו)
# ==========================================

# 1. הגדרת עמוד
st.set_page_config(
    page_title="דשבורד הזמנות",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# הזרקת CSS ל-RTL
st.markdown("""
<style>
    .stApp {
        direction: rtl;
        text-align: right;
    }
    h1, h2, h3, p, div, .stMarkdown, .stRadio, .stSelectbox {
        text-align: right;
    }
    /* תיקון ליישור של המטריקות */
    [data-testid="stMetricValue"] {
        direction: ltr;
        text-align: right; 
    }
    [data-testid="stMetricLabel"] {
        text-align: right;
    }
    /* יישור הטבלה */
    .stDataFrame { direction: rtl; }
</style>
""", unsafe_allow_html=True)

st.title("📦 דשבורד ניהול הזמנות")

# פונקציה לניקוי מספר טלפון לחיפוש
def clean_phone_for_search(phone_input):
    if not phone_input:
        return ""
    clean = re.sub(r'\D', '', str(phone_input))
    if clean.startswith('0'):
        clean = clean[1:]
    return clean

@st.cache_data(ttl=600)
def load_data():
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read()
    
    # טיפול בתאריכים
    if COL_DATE in df.columns:
        df[COL_DATE] = pd.to_datetime(df[COL_DATE], dayfirst=True, errors='coerce')
        df = df.dropna(subset=[COL_DATE])
        df['date_only'] = df[COL_DATE].dt.date
    
    # המרות לטקסט (כדי למנוע שגיאות בחיפוש)
    cols_to_str = [COL_PHONE, COL_SKU, COL_ORDER_NUM]
    for col in cols_to_str:
        if col in df.columns:
            # מנקה .0 אם יש וממיר לטקסט
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True)

    # המרת עמודת כמות למספרים (לצורך סיכום)
    if COL_QUANTITY in df.columns:
        df[COL_QUANTITY] = pd.to_numeric(df[COL_QUANTITY], errors='coerce').fillna(0)

    return df

try:
    df = load_data()
    df_filtered = df.copy()

    # --- סרגל צד (Sidebar) ---
    st.sidebar.header("🔍 סינון וחיפוש")
    
    # 1. סינון תאריכים
    if 'date_only' in df.columns and not df.empty:
        min_date = df['date_only'].min()
        max_date = df['date_only'].max()
        
        if pd.notnull(min_date) and pd.notnull(max_date):
            st.sidebar.subheader("📅 טווח תאריכים")
            # col_d1, col_d2 = st.sidebar.columns(2) # אפשר גם בלי טורים, זה נראה טוב אחד מתחת לשני
            
            start_date = st.sidebar.date_input("מתאריך:", min_date, min_value=min_date, max_value=max_date, format="DD/MM/YYYY")
            end_date = st.sidebar.date_input("עד תאריך:", max_date, min_value=min_date, max_value=max_date, format="DD/MM/YYYY")
            
            if start_date <= end_date:
                mask_date = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
                df_filtered = df_filtered.loc[mask_date]
            else:
                st.sidebar.error("תאריך התחלה מאוחר מתאריך סיום")

    st.sidebar.markdown("---")

    # 2. מנוע חיפוש חכם
    st.sidebar.subheader("🔎 חיפוש מתקדם")
    
    # הגדרת אפשרויות החיפוש
    search_options = {
        "חופשי": "all",
        "מספר הזמנה": COL_ORDER_NUM,
        "מק\"ט": COL_SKU,
        "שם לקוח": COL_CUSTOMER,
        "טלפון": COL_PHONE
    }
    
    search_type_label = st.sidebar.selectbox("חפש לפי:", list(search_options.keys()))
    selected_col = search_options[search_type_label]
    
    search_term = st.sidebar.text_input("הקלד לחיפוש:", placeholder="לדוגמה: 5077...")

    if search_term:
        if selected_col == "all":
            mask = df_filtered.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
            df_filtered = df_filtered[mask]
        
        elif selected_col == COL_PHONE:
            clean_input = clean_phone_for_search(search_term)
            if clean_input:
                mask = df_filtered[COL_PHONE].astype(str).str.contains(clean_input, na=False)
                df_filtered = df_filtered[mask]
                st.sidebar.info(f"מחפש מספר: {clean_input}")
        
        elif selected_col in df_filtered.columns:
            mask = df_filtered[selected_col].astype(str).str.contains(search_term, case=False, na=False)
            df_filtered = df_filtered[mask]
        else:
             st.sidebar.warning(f"העמודה '{selected_col}' לא נמצאה בקובץ.")

    # --- תצוגת מדדים ראשיים (KPIs) ---
    st.markdown("### 📊 נתונים לטווח הנבחר")
    
    total_rows = len(df_filtered)
    
    # חישוב הזמנות רגילות והתקנות
    if COL_SHIP_NUM in df_filtered.columns:
        installs = df_filtered[COL_SHIP_NUM].isna().sum()
        regular = df_filtered[COL_SHIP_NUM].notna().sum()
    else:
        installs = 0
        regular = total_rows

    # חישוב כמות חבילות כוללת
    total_packages = 0
    if COL_QUANTITY in df_filtered.columns:
        total_packages = int(df_filtered[COL_QUANTITY].sum())

    # הצגה ב-4 עמודות
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("📦 סה\"כ רשומות", total_rows)
    kpi2.metric("🔢 סה\"כ חבילות", f"{total_packages:,}") # עם פסיק מפריד אלפים
    kpi3.metric("🚛 הזמנות רגילות", regular)
    kpi4.metric("🛠️ התקנות", installs)
    
    st.divider()

    # --- תצוגת סטטיסטיקה (Top Performers) ---
    if not df_filtered.empty:
        st.markdown("### 🏆 מובילים ומגמות")
        stat1, stat2, stat3 = st.columns(3)
        
        # 1. מק"ט מוביל
        if COL_SKU in df_filtered.columns:
            top_sku = df_filtered[COL_SKU].value_counts()
            if not top_sku.empty:
                best_seller = top_sku.idxmax()
                count_best = top_sku.max()
                
                # המק"ט החלש ביותר (מתוך אלו שנמכרו)
                weakest_seller = top_sku.idxmin()
                count_weak = top_sku.min()
                
                stat1.metric("🌟 המק\"ט הכי נמכר", f"{best_seller}", f"{count_best} פעמים")
                stat2.metric("🐢 המק\"ט הכי חלש", f"{weakest_seller}", f"{count_weak} פעמים")
            else:
                stat1.metric("🌟 המק\"ט הכי נמכר", "-", "-")
                stat2.metric("🐢 המק\"ט הכי חלש", "-", "-")
        
        # 2. לקוח מוביל
        if COL_CUSTOMER in df_filtered.columns:
            top_cust = df_filtered[COL_CUSTOMER].value_counts()
            if not top_cust.empty:
                best_cust = top_cust.idxmax()
                count_cust = top_cust.max()
                stat3.metric("👑 לקוח מוביל", f"{best_cust}", f"{count_cust} הזמנות")

    st.divider()

    # --- הטבלה ---
    st.subheader(f"רשימת הזמנות ({len(df_filtered)} שורות)")
    
    display_df = df_filtered.drop(columns=['date_only'], errors='ignore')
    
    # עיצוב תאריך
    if COL_DATE in display_df.columns:
        display_df[COL_DATE] = display_df[COL_DATE].dt.strftime('%d/%m/%Y')

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)

except Exception as e:
    st.error(f"שגיאה: {e}")
    st.warning("נא לבדוק ששמות העמודות בראש הקוד תואמים בדיוק לקובץ השיטס.")
