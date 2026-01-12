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

# הזרקת CSS
st.markdown("""
<style>
    .stApp {
        direction: rtl;
        text-align: right;
    }
    h1, h2, h3, p, div, .stMarkdown, .stRadio, .stSelectbox, .stTextInput, .stAlert {
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
    
    # עותק בסיסי (לפני סינונים)
    df_filtered = df.copy()

    # --- כותרת ופילטר תאריכים ---
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
                # סינון תאריכים
                mask_date = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
                df_filtered = df_filtered.loc[mask_date]
            else:
                st.error("⚠️ תאריך התחלה מאוחר מתאריך סיום")

    # שמירת נתונים מסונני תאריך (לפני סינון חיפוש) לטובת חישובי אחוזים
    df_date_range_only = df_filtered.copy()
    total_packages_in_date_range = df_date_range_only[COL_QUANTITY].sum()

    st.markdown("---")

    # --- סרגל צד לחיפוש ---
    st.sidebar.header("🔎 חיפוש מתקדם")
    st.sidebar.info("החיפוש מתבצע בתוך טווח התאריכים שנבחר למעלה")
    
    search_options = {
        "מק\"ט": COL_SKU,
        "מספר הזמנה": COL_ORDER_NUM,
        "שם לקוח": COL_CUSTOMER,
        "טלפון": COL_PHONE
    }
    
    search_type_label = st.sidebar.selectbox("חפש לפי:", list(search_options.keys()))
    selected_col = search_options[search_type_label]
    
    placeholder_text = f"הקלד {search_type_label}..."
    search_term = st.sidebar.text_input("ערך לחיפוש:", placeholder=placeholder_text)

    # ביצוע החיפוש בפועל
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

    # --- מדדים ראשיים (KPIs) - מעודכן לחבילות ---
    total_rows = len(df_filtered)
    
    # חישוב חבילות לפי סוג (ולא הזמנות)
    total_packages = int(df_filtered[COL_QUANTITY].sum())
    
    # חישוב חבילות להזמנות רגילות (איפה שיש מספר משלוח)
    regular_mask = df_filtered[COL_SHIP_NUM].notna()
    regular_packages = int(df_filtered.loc[regular_mask, COL_QUANTITY].sum())
    
    # חישוב חבילות להתקנות (איפה שאין מספר משלוח)
    install_mask = df_filtered[COL_SHIP_NUM].isna()
    install_packages = int(df_filtered.loc[install_mask, COL_QUANTITY].sum())

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("📦 סה\"כ רשומות", total_rows)
    kpi2.metric("🔢 סה\"כ חבילות", f"{total_packages:,}")
    kpi3.metric("🚛 הזמנות רגילות (חבילות)", f"{regular_packages:,}")
    kpi4.metric("🛠️ התקנות (חבילות)", f"{install_packages:,}")
    
    # --- תצוגת אחוז נתח שוק בחיפוש ---
    if search_term and total_packages_in_date_range > 0:
        search_share_pct = (total_packages / total_packages_in_date_range) * 100
        st.info(f"📊 תוצאות החיפוש מהוות **{search_share_pct:.1f}%** מסך החבילות בטווח התאריכים הנבחר ({total_packages} מתוך {int(total_packages_in_date_range)})")

    st.markdown("---")

    # --- גרף מגמות ---
    st.subheader("📈 פעילות יומית")
    if 'date_only' in df_filtered.columns and not df_filtered.empty:
        # הקבצה לפי תאריך
        daily_data = df_filtered.groupby('date_only').agg({
            COL_QUANTITY: 'sum',  # סכום חבילות
            COL_SKU: 'count'      # מספר שורות (הזמנות/פריטים)
        }).rename(columns={COL_QUANTITY: 'חבילות', COL_SKU: 'מספר שורות'})
        
        tab1, tab2 = st.tabs(["📝 מספר הזמנות", "📊 כמות חבילות"])
        
        with tab1:
            st.caption("מספר הרשומות/הזמנות לכל יום (גרף קווי)")
            st.line_chart(daily_data['מספר שורות'], color="#E74C3C") 

        with tab2:
            st.caption("כמות החבילות הכוללת לכל יום (גרף עמודות)")
            st.bar_chart(daily_data['חבילות'], color="#2E86C1") 
            
    else:
        st.info("אין מספיק נתונים להצגת גרף")

    st.markdown("---")

    # --- סטטיסטיקה מהירה + טבלאות ---
    if not df_filtered.empty and COL_SKU in df_filtered.columns and COL_QUANTITY in df_filtered.columns:
        
        # חישוב סטטיסטיקות מק"ט
        sku_stats = df_filtered.groupby(COL_SKU)[COL_QUANTITY].sum().reset_index()
        total_q_current = df_filtered[COL_QUANTITY].sum()
        
        if not sku_stats.empty:
            # מק"ט מוביל
            best_sku_row = sku_stats.loc[sku_stats[COL_QUANTITY].idxmax()]
            best_seller = best_sku_row[COL_SKU]
            count_best = int(best_sku_row[COL_QUANTITY])
            
            st.metric("🌟 המק\"ט הכי נמכר", f"{best_seller}", f"{count_best} חבילות")
            
            st.divider()
            
            col_top, col_bottom = st.columns(2)
            
            with col_top:
                st.subheader("🏆 5 המוצרים המובילים")
                top_5 = sku_stats.sort_values(by=COL_QUANTITY, ascending=False).head(5).copy()
                if total_q_current > 0:
                    top_5['נתח שוק (%)'] = (top_5[COL_QUANTITY] / total_q_current * 100).round(1).astype(str) + '%'
                top_5 = top_5.rename(columns={COL_SKU: 'מק"ט', COL_QUANTITY: 'חבילות'})
                st.dataframe(top_5, hide_index=True, use_container_width=True)

            with col_bottom:
                st.subheader("🐢 3 המוצרים החלשים")
                # לוקחים את ה-3 עם הכמות הכי נמוכה (אבל שגדולים מ-0, כי הם קיימים ברשימה)
                bottom_3 = sku_stats.sort_values(by=COL_QUANTITY, ascending=True).head(3).copy()
                if total_q_current > 0:
                    bottom_3['נתח שוק (%)'] = (bottom_3[COL_QUANTITY] / total_q_current * 100).round(1).astype(str) + '%'
                bottom_3 = bottom_3.rename(columns={COL_SKU: 'מק"ט', COL_QUANTITY: 'חבילות'})
                st.dataframe(bottom_3, hide_index=True, use_container_width=True)

    else:
        st.warning("אין מספיק נתונים לחישוב סטטיסטיקות")

    st.markdown("---")

    # --- טבלה ראשית ---
    st.subheader(f"רשימת הזמנות מלאה ({len(df_filtered)})")
    display_df = df_filtered.drop(columns=['date_only'], errors='ignore')
    
    if COL_DATE in display_df.columns:
        display_df[COL_DATE] = display_df[COL_DATE].dt.strftime('%d/%m/%Y')

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)

except Exception as e:
    st.error(f"שגיאה: {e}")
