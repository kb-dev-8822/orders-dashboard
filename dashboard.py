import streamlit as st
import pandas as pd
import psycopg2
import re
from datetime import datetime, timedelta

# ==========================================
# 1. הגדרות עמוד
# ==========================================
st.set_page_config(
    page_title="דשבורד הזמנות",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"  # סרגל צד פתוח תמיד
)

# ==========================================
# ⚙️ משיכת פרטי חיבור מהסודות (Secrets)
# ==========================================
try:
    DB_HOST = st.secrets["supabase"]["DB_HOST"]
    DB_PORT = st.secrets["supabase"]["DB_PORT"]
    DB_NAME = st.secrets["supabase"]["DB_NAME"]
    DB_USER = st.secrets["supabase"]["DB_USER"]
    DB_PASS = st.secrets["supabase"]["DB_PASS"]
except Exception:
    st.error("❌ שגיאה: חסרים פרטי חיבור ל-Supabase בקובץ secrets.toml")
    st.stop()

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
COL_CITY = 'עיר'
COL_STREET = 'רחוב'
COL_HOUSE = 'מספר בית'

# ==========================================
# 🎨 CSS להעלמת כפתור הסגירה (ה"נעילה") ועיצוב RTL
# ==========================================
st.markdown("""
<style>
    /* כיוון ימין-שמאל כללי */
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
    
    /* --- קוד הנעילה של הסרגל --- */
    [data-testid="stSidebarCollapsedControl"] {
        display: none !important;
    }
    section[data-testid="stSidebar"] > div > div:first-child button {
        display: none !important;
    }
    section[data-testid="stSidebar"] {
        direction: rtl;
    }
</style>
""", unsafe_allow_html=True)

# --- מנגנון אבטחה (Login) ---
def check_password():
    if "app_password" not in st.secrets:
        st.warning("⚠️ לא הוגדרה סיסמה ב-Secrets. הכניסה חופשית.")
        return True

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
def load_data_from_sql():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            sslmode='require'
        )
        query = """
            SELECT 
                order_num, 
                customer_name, 
                phone, 
                city, 
                street, 
                house_num, 
                sku, 
                quantity, 
                shipping_num, 
                order_date 
            FROM orders
        """
        df = pd.read_sql(query, conn)
        conn.close()

        # המרה לעברית
        df = df.rename(columns={
            'order_num': COL_ORDER_NUM,
            'customer_name': COL_CUSTOMER,
            'phone': COL_PHONE,
            'city': COL_CITY,
            'street': COL_STREET,
            'house_num': COL_HOUSE,
            'sku': COL_SKU,
            'quantity': COL_QUANTITY,
            'shipping_num': COL_SHIP_NUM,
            'order_date': COL_DATE
        })

        # טיפול בתאריכים
        df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors='coerce')
        df = df.dropna(subset=[COL_DATE])
        df['date_only'] = df[COL_DATE].dt.date
        
        # המרה לטקסט
        cols_to_str = [COL_SKU, COL_ORDER_NUM, COL_SHIP_NUM]
        for col in cols_to_str:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.replace(r'\.0$', '', regex=True)

        if COL_PHONE in df.columns:
            df[COL_PHONE] = df[COL_PHONE].apply(normalize_phone_str)

        if COL_QUANTITY in df.columns:
            df[COL_QUANTITY] = pd.to_numeric(df[COL_QUANTITY], errors='coerce').fillna(0)

        # ---------------------------------------------------------
        #  ✨ כאן מתבצע הניקוי החכם של המק"טים (Normalization) ✨
        # ---------------------------------------------------------
        if COL_SKU in df.columns:
            # 1. המרה לאותיות גדולות (מטפל ב-White vs WHITE)
            df[COL_SKU] = df[COL_SKU].astype(str).str.upper()
            
            # 2. החלפת לוכסנים ברווח (מטפל ב-WOOD/BLACK)
            df[COL_SKU] = df[COL_SKU].str.replace('/', ' ', regex=False)
            df[COL_SKU] = df[COL_SKU].str.replace('\\', ' ', regex=False)
            
            # 3. ניקוי רווחים כפולים ורווחים בקצוות
            df[COL_SKU] = df[COL_SKU].str.replace(r'\s+', ' ', regex=True).str.strip()
        # ---------------------------------------------------------

        return df

    except Exception as e:
        st.error(f"שגיאה בחיבור למסד הנתונים: {e}")
        return pd.DataFrame()

try:
    if st.sidebar.button("🔄 רענן נתונים עכשיו"):
        load_data_from_sql.clear()
        st.rerun()

    df = load_data_from_sql()
    
    df_filtered = df.copy()

    st.title("📦 דשבורד ניהול הזמנות (SQL)")

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

    df_date_range_only = df_filtered.copy()
    total_packages_in_date_range = df_date_range_only[COL_QUANTITY].sum()

    st.markdown("---")

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

    if search_term:
        # --- תוספת: נרמול חכם לחיפוש מק"ט ---
        if selected_col == COL_SKU:
            # מבצעים בדיוק את אותו ניקוי שעשינו לנתונים בטעינה
            search_term = search_term.upper()                # אותיות גדולות
            search_term = search_term.replace('/', ' ')      # החלפת / ברווח
            search_term = search_term.replace('\\', ' ')     # החלפת \ ברווח
            search_term = re.sub(r'\s+', ' ', search_term).strip() # ניקוי רווחים כפולים
            
            st.sidebar.caption(f"🔎 מחפש בפועל: {search_term}")

        # --- לוגיקה קיימת ---
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

    total_rows = len(df_filtered)
    total_packages = int(df_filtered[COL_QUANTITY].sum())
    
    regular_mask = df_filtered[COL_SHIP_NUM].str.strip() != ""
    regular_packages = int(df_filtered.loc[regular_mask, COL_QUANTITY].sum())
    
    install_mask = df_filtered[COL_SHIP_NUM].str.strip() == ""
    install_packages = int(df_filtered.loc[install_mask, COL_QUANTITY].sum())

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("📦 סה\"כ רשומות", total_rows)
    kpi2.metric("🔢 סה\"כ חבילות", f"{total_packages:,}")
    kpi3.metric("🚛 הזמנות רגילות (חבילות)", f"{regular_packages:,}")
    kpi4.metric("🛠️ התקנות (חבילות)", f"{install_packages:,}")
    
    if search_term and total_packages_in_date_range > 0:
        search_share_pct = (total_packages / total_packages_in_date_range) * 100
        st.info(f"📊 תוצאות החיפוש מהוות **{search_share_pct:.1f}%** מסך החבילות בטווח התאריכים הנבחר")

    st.markdown("---")

    if not df_filtered.empty and COL_SKU in df_filtered.columns and COL_QUANTITY in df_filtered.columns:
        
        sku_stats = df_filtered.groupby(COL_SKU)[COL_QUANTITY].sum().reset_index()
        total_q_current = df_filtered[COL_QUANTITY].sum()
        
        if not sku_stats.empty:
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
                st.subheader("🐢 5 המוצרים החלשים")
                bottom_5 = sku_stats.sort_values(by=COL_QUANTITY, ascending=True).head(5).copy()
                if total_q_current > 0:
                    bottom_5['נתח שוק (%)'] = (bottom_5[COL_QUANTITY] / total_q_current * 100).round(1).astype(str) + '%'
                bottom_5 = bottom_5.rename(columns={COL_SKU: 'מק"ט', COL_QUANTITY: 'חבילות'})
                st.dataframe(bottom_5, hide_index=True, use_container_width=True)

    else:
        st.warning("אין מספיק נתונים לחישוב סטטיסטיקות")

    st.markdown("---")

    st.subheader("📈 פעילות יומית")
    if 'date_only' in df_filtered.columns and not df_filtered.empty:
        daily_data = df_filtered.groupby('date_only').agg({
            COL_QUANTITY: 'sum',  
            COL_SKU: 'count'
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

    st.subheader(f"רשימת הזמנות מלאה ({len(df_filtered)})")
    
    display_cols = [COL_DATE, COL_ORDER_NUM, COL_CUSTOMER, COL_PHONE, COL_CITY, COL_STREET, COL_HOUSE, COL_SKU, COL_QUANTITY, COL_SHIP_NUM]
    final_cols = [c for c in display_cols if c in df_filtered.columns]
    
    display_df = df_filtered[final_cols].copy()
    
    if COL_DATE in display_df.columns:
        display_df[COL_DATE] = display_df[COL_DATE].dt.strftime('%d/%m/%Y')

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)

except Exception as e:
    st.error(f"שגיאה כללית בדשבורד: {e}")
