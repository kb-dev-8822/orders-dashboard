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

# 1. הגדרת עמוד (חייב להיות ראשון)
st.set_page_config(
    page_title="דשבורד הזמנות",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# הזרקת CSS (כולל עיצוב למסך כניסה)
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
    .stButton button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

# --- מנגנון אבטחה (Login) ---
def check_password():
    if "app_password" not in st.secrets:
        st.error("⚠️ לא הוגדרה סיסמה ב-Secrets. נא להוסיף 'app_password'.")
        return False

    def password_entered():
        if st.session_state["password"] == st.secrets["app_password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown("### 🔒 התחברות למערכת")
        st.text_input(
            "הזמן סיסמה", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        st.markdown("### 🔒 התחברות למערכת")
        st.text_input(
            "הזמן סיסמה", type="password", on_change=password_entered, key="password"
        )
        st.error("❌ סיסמה שגויה")
        return False
    else:
        return True

if not check_password():
    st.stop()

# ========================================================
# מכאן והלאה - הקוד של הדשבורד
# ========================================================

def normalize_phone_str(phone_val):
    if pd.isna(phone_val) or phone_val == "":
        return ""
    s = str(phone_val)
    s = s.replace('.0', '')
    clean = re.sub(r'\D', '', s)
    if clean.startswith('0'):
        clean = clean[1:]
    return clean

@st.cache_data(ttl=600)
def load_data():
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read()
    
    if COL_DATE in df.columns:
        df[COL_DATE] = pd.to_datetime(df[COL_DATE], dayfirst=True, errors='coerce')
        df = df.dropna(subset=[COL_DATE])
        df['date_only'] = df[COL_DATE].dt.date
    
    cols_to_str = [COL_SKU, COL_ORDER_NUM]
    for col in cols_to_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True)

    if COL_PHONE in df.columns:
        df[COL_PHONE] = df[COL_PHONE].apply(normalize_phone_str)

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
        "מספר הזמנה": COL_ORDER_NUM,
        "מק\"ט": COL_SKU,
        "שם לקוח": COL_CUSTOMER,
        "טלפון": COL_PHONE
    }
    
    search_type_label = st.sidebar.selectbox("חפש לפי:", list(search_options.keys()))
    selected_col = search_options[search_type_label]
    
    placeholder_text = f"הקלד {search_type_label}..."
    search_term = st.sidebar.text_input("ערך לחיפוש:", placeholder=placeholder_text)

    if search_term:
        if selected_col == COL_PHONE:
            clean_input = normalize_phone_str(search_term)
            st.sidebar.caption(f"מחפש מספר מנורמל: {clean_input}")
            mask = df_filtered[COL_PHONE].astype(str).str.contains(clean_input, na=False)
            df_filtered = df_filtered[mask]

        elif selected_col in df_filtered.columns:
            mask = df_filtered[selected_col].astype(str).str.contains(search_term, case=False, na=False)
            df_filtered = df_filtered[mask]
        else:
             st.sidebar.warning(f"העמודה '{selected_col}' לא נמצאה.")

    # --- מדדים ראשיים (KPIs) ---
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
    
    st.markdown("---")

    # --- גרף מגמות (משודרג: טאבים + עמודות) ---
    st.subheader("📈 פעילות יומית")
    if 'date_only' in df_filtered.columns and not df_filtered.empty:
        # הקבצה לפי תאריך
        daily_data = df_filtered.groupby('date_only').agg({
            COL_QUANTITY: 'sum',  # סכום חבילות
            COL_SKU: 'count'      # מספר שורות (הזמנות/פריטים)
        }).rename(columns={COL_QUANTITY: 'חבילות', COL_SKU: 'מספר שורות'})
        
        # שימוש בטאבים כדי לא להעמיס וכדי למנוע בלאגן בגרף
        tab1, tab2 = st.tabs(["📊 כמות חבילות", "📝 מספר הזמנות"])
        
        with tab1:
            st.caption("כמות החבילות הכוללת לכל יום (גרף עמודות)")
            st.bar_chart(daily_data['חבילות'], color="#2E86C1") # צבע כחול מקצועי
            
        with tab2:
            st.caption("מספר הרשומות/הזמנות לכל יום (גרף קווי)")
            st.line_chart(daily_data['מספר שורות'], color="#E74C3C") # צבע אדום מקצועי
            
    else:
        st.info("אין מספיק נתונים להצגת גרף")

    st.markdown("---")

    # --- סטטיסטיקה מהירה ---
    if not df_filtered.empty:
        stat1, stat2, stat3 = st.columns(3)
        
        # מק"ט מוביל
        if COL_SKU in df_filtered.columns:
            top_sku = df_filtered[COL_SKU].value_counts()
            if not top_sku.empty:
                best_seller = top_sku.idxmax()
                count_best = top_sku.max()
                weakest_seller = top_sku.idxmin()
                count_weak = top_sku.min()
                stat1.metric("🌟 המק\"ט הכי נמכר", f"{best_seller}", f"{count_best} פעמים")
                stat2.metric("🐢 המק\"ט הכי חלש", f"{weakest_seller}", f"{count_weak} פעמים")
            else:
                stat1.metric("🌟 המק\"ט הכי נמכר", "-", "-")
                stat2.metric("🐢 המק\"ט הכי חלש", "-", "-")
        
        # לקוח מוביל
        if COL_CUSTOMER in df_filtered.columns:
            top_cust = df_filtered[COL_CUSTOMER].value_counts()
            if not top_cust.empty:
                best_cust = top_cust.idxmax()
                count_cust = top_cust.max()
                stat3.metric("👑 לקוח מוביל", f"{best_cust}", f"{count_cust} הזמנות")

    # --- רשימת 5 המק"טים המובילים ---
    with st.expander("🏆 5 המוצרים הנמכרים ביותר (לחץ לפירוט)", expanded=False):
        if COL_SKU in df_filtered.columns and COL_QUANTITY in df_filtered.columns:
            # קיבוץ לפי מק"ט וסיכום כמויות
            sku_stats = df_filtered.groupby(COL_SKU)[COL_QUANTITY].sum().reset_index()
            sku_stats = sku_stats.sort_values(by=COL_QUANTITY, ascending=False).head(5)
            
            total_q = df_filtered[COL_QUANTITY].sum()
            if total_q > 0:
                sku_stats['נתח שוק (%)'] = (sku_stats[COL_QUANTITY] / total_q * 100).round(1).astype(str) + '%'
            
            sku_stats = sku_stats.rename(columns={COL_SKU: 'מק"ט', COL_QUANTITY: 'סה"כ חבילות שנמכרו'})
            st.dataframe(sku_stats, hide_index=True, use_container_width=True)
        else:
            st.warning("חסרים נתונים לחישוב מק\"טים מובילים")

    st.markdown("---")

    # --- טבלה ראשית ---
    st.subheader(f"רשימת הזמנות מלאה ({len(df_filtered)})")
    display_df = df_filtered.drop(columns=['date_only'], errors='ignore')
    
    if COL_DATE in display_df.columns:
        display_df[COL_DATE] = display_df[COL_DATE].dt.strftime('%d/%m/%Y')

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)

except Exception as e:
    st.error(f"שגיאה: {e}")
