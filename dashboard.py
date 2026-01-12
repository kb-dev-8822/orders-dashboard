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
    initial_sidebar_state="collapsed"
)

# הזרקת CSS
st.markdown("""
<style>
    .stApp {
        direction: rtl;
        text-align: right;
    }
    h1, h2, h3, p, div, .stMarkdown, .stRadio, .stSelectbox, .stTextInput {
        text-align: right;
    }
    [data-testid="stMetricValue"] {
        direction: ltr;
        text-align: right; 
    }
    [data-testid="stMetricLabel"] {
        text-align: right;
    }
</style>
""", unsafe_allow_html=True)

# --- פונקציית הקסם לנרמול טלפונים ---
def normalize_phone_str(phone_val):
    """
    מקבל כל דבר (מספר, טקסט עם מקפים וכו')
    ומחזיר מחרוזת נקייה של ספרות בלבד ללא אפס מוביל
    """
    if pd.isna(phone_val) or phone_val == "":
        return ""
    
    # המרה לטקסט
    s = str(phone_val)
    # ניקוי סיומת עשרונית אם יש (.0)
    s = s.replace('.0', '')
    # השארת ספרות בלבד (מעיף מקפים, רווחים, סוגריים וכו')
    clean = re.sub(r'\D', '', s)
    
    # הסרת אפס מוביל אם יש
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
    
    # המרות לטקסט ונרמול
    cols_to_str = [COL_SKU, COL_ORDER_NUM]
    for col in cols_to_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True)

    # נרמול אגרסיבי לעמודת הטלפון כבר בטעינה!
    # אנחנו שומרים את הערך המנורמל בעמודה המקורית כדי שהחיפוש יעבוד חלק
    if COL_PHONE in df.columns:
        df[COL_PHONE] = df[COL_PHONE].apply(normalize_phone_str)

    # המרת כמות למספרים
    if COL_QUANTITY in df.columns:
        df[COL_QUANTITY] = pd.to_numeric(df[COL_QUANTITY], errors='coerce').fillna(0)

    return df

try:
    df = load_data()
    df_filtered = df.copy()

    # --- כותרת ופילטר עליון ---
    st.title("📦 דשבורד ניהול הזמנות")

    with st.container():
        st.markdown("### 📅 סינון לפי תאריכים")
        
        default_end = datetime.now().date()
        default_start = default_end - timedelta(days=30)
        
        # עדכון ברירת מחדל אם יש נתונים, אבל בלי לנעול גבולות
        if 'date_only' in df.columns and not df.empty:
            data_min = df['date_only'].min()
            data_max = df['date_only'].max()
            if pd.notnull(data_min): default_start = data_min
            if pd.notnull(data_max): default_end = data_max

        col_filter1, col_filter2, col_spacer = st.columns([1, 1, 2])
        
        with col_filter1:
            start_date = st.date_input("מתאריך:", value=default_start, format="DD/MM/YYYY")
        with col_filter2:
            end_date = st.date_input("עד תאריך:", value=default_end, format="DD/MM/YYYY")

        if start_date and end_date:
            if start_date <= end_date:
                mask_date = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
                df_filtered = df_filtered.loc[mask_date]
            else:
                st.error("⚠️ תאריך התחלה מאוחר מתאריך סיום")

    st.markdown("---")

    # --- סרגל צד לחיפוש ---
    st.sidebar.header("🔎 חיפוש מתקדם")
    st.sidebar.info("החיפוש מתבצע בתוך טווח התאריכים שנבחר למעלה")
    
    search_options = {
        "חופשי": "all",
        "מספר הזמנה": COL_ORDER_NUM,
        "מק\"ט": COL_SKU,
        "שם לקוח": COL_CUSTOMER,
        "טלפון": COL_PHONE
    }
    
    search_type_label = st.sidebar.selectbox("חפש לפי:", list(search_options.keys()))
    selected_col = search_options[search_type_label]
    search_term = st.sidebar.text_input("הקלד לחיפוש:", placeholder="לדוגמה: 050-12345...")

    if search_term:
        # לוגיקה ייחודית לכל סוג חיפוש
        
        if selected_col == COL_PHONE:
            # 1. מנרמלים את מה שהמשתמש הקליד (מורידים מקפים, 0 וכו')
            clean_input = normalize_phone_str(search_term)
            st.sidebar.caption(f"מחפש מספר מנורמל: {clean_input}")
            
            # 2. מחפשים את המספר הנקי בתוך העמודה (שכבר ניקינו בטעינה)
            # מכיוון ששני הצדדים נקיים, זה יעבוד מושלם
            mask = df_filtered[COL_PHONE].astype(str).str.contains(clean_input, na=False)
            df_filtered = df_filtered[mask]

        elif selected_col == "all":
            # חיפוש כללי
            mask = df_filtered.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
            df_filtered = df_filtered[mask]

        elif selected_col in df_filtered.columns:
            # חיפוש רגיל (מספר הזמנה, מקט, שם)
            # case=False מבטיח התעלמות מאותיות גדולות/קטנות
            mask = df_filtered[selected_col].astype(str).str.contains(search_term, case=False, na=False)
            df_filtered = df_filtered[mask]
        else:
             st.sidebar.warning(f"העמודה '{selected_col}' לא נמצאה.")

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
                stat1.metric("🌟 המק\"ט הכי נמכר", f"{best_seller}", f"{count_best}")
                stat2.metric("🐢 המק\"ט הכי חלש", f"{weakest_seller}", f"{count_weak}")
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
