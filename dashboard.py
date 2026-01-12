import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection
import re
from datetime import datetime, timedelta

# ==========================================
# ⚙️ הגדרות שמות עמודות
# ==========================================
COL_SKU = 'מקט'
COL_CUSTOMER = 'שם פרטי'
COL_PHONE = 'טלפון'
COL_ORDER_NUM = 'מספר הזמנה'
COL_QUANTITY = 'כמות'
COL_DATE = 'תאריך'
COL_SHIP_NUM = 'מספר משלוח'
# ==========================================

# 1. הגדרת עמוד
st.set_page_config(
    page_title="דשבורד הזמנות",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed" # סוגר את הבר הצדדי כברירת מחדל כדי לתת מקום
)

# הזרקת CSS ל-RTL ולעיצוב נקי יותר
st.markdown("""
<style>
    .stApp {
        direction: rtl;
        text-align: right;
    }
    h1, h2, h3, p, div, .stMarkdown, .stRadio, .stSelectbox, .stTextInput {
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
    /* עיצוב לבר העליון של הפילטרים */
    .filter-container {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# פונקציה לניקוי מספר טלפון
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
    
    # המרות לטקסט
    cols_to_str = [COL_PHONE, COL_SKU, COL_ORDER_NUM]
    for col in cols_to_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True)

    # המרת כמות למספרים
    if COL_QUANTITY in df.columns:
        df[COL_QUANTITY] = pd.to_numeric(df[COL_QUANTITY], errors='coerce').fillna(0)

    return df

try:
    df = load_data()
    df_filtered = df.copy()

    # --- כותרת ראשית ---
    st.title("📦 דשבורד ניהול הזמנות")

    # --- אזור סינון עליון (Top Bar) ---
    # נשתמש ב-Container כדי לרכז את הסינונים למעלה
    with st.container():
        st.markdown("### 📅 סינון לפי תאריכים")
        
        # חישוב תאריכי ברירת מחדל (חודש אחרון אם אין בחירה אחרת)
        default_end = datetime.now().date()
        default_start = default_end - timedelta(days=30)
        
        # אם יש נתונים, ננסה לקחת את התאריכים מהקובץ כברירת מחדל, אבל לא ננעל את הלוח
        if 'date_only' in df.columns and not df.empty:
            data_min = df['date_only'].min()
            data_max = df['date_only'].max()
            # משתמשים בנתונים רק כברירת מחדל (Value), לא כגבול (Min/Max)
            if pd.notnull(data_min): default_start = data_min
            if pd.notnull(data_max): default_end = data_max

        col_filter1, col_filter2, col_spacer = st.columns([1, 1, 2])
        
        with col_filter1:
            # הסרתי את min_value ו-max_value כדי לאפשר בחירה חופשית לחלוטין (כולל 2026)
            start_date = st.date_input("מתאריך:", value=default_start, format="DD/MM/YYYY")
        
        with col_filter2:
            end_date = st.date_input("עד תאריך:", value=default_end, format="DD/MM/YYYY")

        # ביצוע הסינון
        if start_date and end_date:
            if start_date <= end_date:
                mask_date = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
                df_filtered = df_filtered.loc[mask_date]
            else:
                st.error("⚠️ שים לב: תאריך ההתחלה מאוחר מתאריך הסיום")

    st.markdown("---")

    # --- סרגל צד (Sidebar) - נשאר רק לחיפוש ספציפי ---
    st.sidebar.header("🔎 חיפוש מתקדם")
    st.sidebar.info("כאן אפשר לחפש הזמנה ספציפית בתוך הטווח שנבחר")
    
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

    # לוגיקת החיפוש
    if search_term:
        if selected_col == "all":
            mask = df_filtered.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
            df_filtered = df_filtered[mask]
        elif selected_col == COL_PHONE:
            clean_input = clean_phone_for_search(search_term)
            if clean_input:
                mask = df_filtered[COL_PHONE].astype(str).str.contains(clean_input, na=False)
                df_filtered = df_filtered[mask]
        elif selected_col in df_filtered.columns:
            mask = df_filtered[selected_col].astype(str).str.contains(search_term, case=False, na=False)
            df_filtered = df_filtered[mask]

    # --- מדדים (KPIs) ---
    total_rows = len(df_filtered)
    if COL_SHIP_NUM in df_filtered.columns:
        installs = df_filtered[COL_SHIP_NUM].isna().sum()
        regular = df_filtered[COL_SHIP_NUM].notna().sum()
    else:
        installs = 0
        regular = total_rows

    total_packages = 0
    if COL_QUANTITY in df_filtered.columns:
        total_packages = int(df_filtered[COL_QUANTITY].sum())

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("📦 סה\"כ רשומות", total_rows)
    kpi2.metric("🔢 סה\"כ חבילות", f"{total_packages:,}")
    kpi3.metric("🚛 הזמנות רגילות", regular)
    kpi4.metric("🛠️ התקנות", installs)
    
    st.divider()

    # --- סטטיסטיקה ---
    if not df_filtered.empty:
        stat1, stat2, stat3 = st.columns(3)
        
        # מק"ט
        if COL_SKU in df_filtered.columns:
            top_sku = df_filtered[COL_SKU].value_counts()
            if not top_sku.empty:
                best_seller = top_sku.idxmax()
                count_best = top_sku.max()
                weakest_seller = top_sku.idxmin()
                count_weak = top_sku.min()
                stat1.metric("🌟 המק\"ט הכי נמכר", f"{best_seller}", f"{count_best} יחידות")
                stat2.metric("🐢 המק\"ט הכי חלש", f"{weakest_seller}", f"{count_weak} יחידות")
            else:
                stat1.metric("🌟 המק\"ט הכי נמכר", "-", "-")
                stat2.metric("🐢 המק\"ט הכי חלש", "-", "-")
        
        # לקוח
        if COL_CUSTOMER in df_filtered.columns:
            top_cust = df_filtered[COL_CUSTOMER].value_counts()
            if not top_cust.empty:
                best_cust = top_cust.idxmax()
                count_cust = top_cust.max()
                stat3.metric("👑 לקוח מוביל", f"{best_cust}", f"{count_cust} הזמנות")

    # --- טבלה ---
    st.subheader(f"רשימת הזמנות ({len(df_filtered)})")
    display_df = df_filtered.drop(columns=['date_only'], errors='ignore')
    if COL_DATE in display_df.columns:
        display_df[COL_DATE] = display_df[COL_DATE].dt.strftime('%d/%m/%Y')

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)

except Exception as e:
    st.error(f"שגיאה: {e}")
