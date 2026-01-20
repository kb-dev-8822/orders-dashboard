import streamlit as st
import pandas as pd
import psycopg2
import re
import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime # <--- הוספתי לטיפול בתאריך מהמייל
import io
import os
import numpy as np
from datetime import datetime, timedelta
import calendar
from zoneinfo import ZoneInfo

# ==========================================
# 1. הגדרות עמוד
# ==========================================
st.set_page_config(
    page_title="דשבורד הזמנות ומלאי",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# ⚙️ הגדרות שמות עמודות וקבצים
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

INVENTORY_CACHE_FILE = "inventory_cache.csv"
INVENTORY_DATE_FILE = "inventory_date.txt" # <--- קובץ לשמירת תאריך המלאי

# ==========================================
# 🎨 CSS
# ==========================================
st.markdown("""
<style>
    .stApp { direction: rtl; text-align: right; }
    h1, h2, h3, p, div, .stMarkdown, .stRadio, .stSelectbox, .stTextInput, .stAlert, .stNumberInput { text-align: right; }
    [data-testid="stMetricValue"] { direction: ltr; text-align: right; }
    [data-testid="stMetricLabel"] { text-align: right; }
    .stButton button { width: 100%; }
    
    [data-testid="stSidebarCollapsedControl"] { display: none !important; }
    section[data-testid="stSidebar"] > div > div:first-child button { display: none !important; }
    section[data-testid="stSidebar"] { direction: rtl; }
    
    thead tr th:first-child {display:none}
    tbody th {display:none}
</style>
""", unsafe_allow_html=True)

# ==========================================
# 🔐 מנגנון אבטחה
# ==========================================
def check_password():
    if "app_password" not in st.secrets:
        return True

    def password_entered():
        if st.session_state["password"] == st.secrets["app_password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown("### 🔒 התחברות למערכת")
        st.text_input("הזמן סיסמה", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown("### 🔒 התחברות למערכת")
        st.text_input("הזמן סיסמה", type="password", on_change=password_entered, key="password")
        st.error("❌ סיסמה שגויה")
        return False
    else:
        return True

if not check_password():
    st.stop()

# ==========================================
# 🛠️ פונקציות עזר
# ==========================================

def normalize_phone_str(phone_val):
    if pd.isna(phone_val) or str(phone_val).strip() == "":
        return ""
    s = str(phone_val).replace('.0', '').strip()
    clean = re.sub(r'\D', '', s)
    if not clean:
        return ""
    if not clean.startswith('0'):
        clean = '0' + clean
    return clean

def clean_sku(val):
    if pd.isna(val): return ""
    val = str(val).upper()
    val = val.replace('/', ' ').replace('\\', ' ')
    val = re.sub(r'\s+', ' ', val).strip()
    return val

# ==========================================
# 📥 טעינת נתונים (SQL + Email + Cache)
# ==========================================

@st.cache_data
def load_data_from_sql():
    try:
        conn = psycopg2.connect(
            host=st.secrets["supabase"]["DB_HOST"],
            port=st.secrets["supabase"]["DB_PORT"],
            database=st.secrets["supabase"]["DB_NAME"],
            user=st.secrets["supabase"]["DB_USER"],
            password=st.secrets["supabase"]["DB_PASS"],
            sslmode='require'
        )
        
        # 1. שליפת הזמנות רגילות
        query_orders = """
            SELECT order_num, customer_name, phone, city, street, house_num, sku, quantity, shipping_num, order_date 
            FROM orders
        """
        df = pd.read_sql(query_orders, conn)
        
        # 2. שליפת הזמנות עתידיות (Pre-Orders)
        query_pre = "SELECT sku, quantity FROM pre_orders"
        try:
            df_pre = pd.read_sql(query_pre, conn)
        except Exception:
            df_pre = pd.DataFrame(columns=['sku', 'quantity'])
            
        conn.close()

        # עיבוד הזמנות רגילות
        df = df.rename(columns={
            'order_num': COL_ORDER_NUM, 'customer_name': COL_CUSTOMER, 'phone': COL_PHONE,
            'city': COL_CITY, 'street': COL_STREET, 'house_num': COL_HOUSE,
            'sku': COL_SKU, 'quantity': COL_QUANTITY, 'shipping_num': COL_SHIP_NUM, 'order_date': COL_DATE
        })

        df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors='coerce')
        df = df.dropna(subset=[COL_DATE])
        df['date_only'] = df[COL_DATE].dt.date

        cols_to_str = [COL_SKU, COL_ORDER_NUM, COL_SHIP_NUM]
        for col in cols_to_str:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.replace(r'\.0$', '', regex=True)

        if COL_PHONE in df.columns:
            df[COL_PHONE] = df[COL_PHONE].apply(normalize_phone_str)

        if COL_QUANTITY in df.columns:
            df[COL_QUANTITY] = pd.to_numeric(df[COL_QUANTITY], errors='coerce').fillna(0).astype(int)

        if COL_SKU in df.columns:
            df[COL_SKU] = df[COL_SKU].apply(clean_sku)
            
        # עיבוד pre-orders
        if not df_pre.empty:
            df_pre['sku'] = df_pre['sku'].apply(clean_sku)
            df_pre['quantity'] = pd.to_numeric(df_pre['quantity'], errors='coerce').fillna(0).astype(int)
            df_pre_grouped = df_pre.groupby('sku')['quantity'].sum().reset_index().rename(columns={'quantity': 'backlog_qty'})
        else:
            df_pre_grouped = pd.DataFrame(columns=['sku', 'backlog_qty'])

        return df, df_pre_grouped

    except Exception as e:
        st.error(f"שגיאה בחיבור למסד הנתונים: {e}")
        return pd.DataFrame(), pd.DataFrame()

def load_inventory_cache():
    # טעינת הטבלה
    df_inv = None
    if os.path.exists(INVENTORY_CACHE_FILE):
        try:
            df_inv = pd.read_csv(INVENTORY_CACHE_FILE)
            if COL_SKU in df_inv.columns:
                df_inv[COL_SKU] = df_inv[COL_SKU].apply(clean_sku)
        except Exception:
            df_inv = None
            
    # טעינת התאריך
    inv_date = None
    if os.path.exists(INVENTORY_DATE_FILE):
        try:
            with open(INVENTORY_DATE_FILE, "r") as f:
                inv_date = f.read().strip()
        except:
            inv_date = None
            
    return df_inv, inv_date

def fetch_inventory_from_email():
    if "email" not in st.secrets:
        st.error("חסרים פרטי אימייל ב-secrets.toml")
        return None, None

    EMAIL_USER = st.secrets["email"]["user"]
    EMAIL_PASS = st.secrets["email"]["password"]
    TARGET_SENDER = st.secrets["email"].get("sender_email", "GlobusInfo@globus-intr.co.il")
    TARGET_SUBJECT = "מלאי סלים פרייס"
    FILE_TO_FIND = "stock122.xlsx"

    status_container = st.empty()
    status_container.info("🔄 מושך מלאי...")

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")

        status, messages = mail.search(None, f'FROM "{TARGET_SENDER}"')
        if not messages[0]:
            status_container.warning(f"לא נמצאו מיילים מ-{TARGET_SENDER}")
            return None, None

        email_ids = messages[0].split()
        
        for eid in reversed(email_ids[-10:]):
            _, msg_data = mail.fetch(eid, "(RFC822)")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding if encoding else "utf-8")
                    
                    if TARGET_SUBJECT in subject:
                        for part in msg.walk():
                            if part.get_content_maintype() == "multipart": continue
                            if part.get("Content-Disposition") is None: continue
                            
                            filename = part.get_filename()
                            if filename:
                                filename, encoding = decode_header(filename)[0]
                                if isinstance(filename, bytes):
                                    filename = filename.decode(encoding if encoding else "utf-8")
                                
                                if FILE_TO_FIND in filename:
                                    file_data = part.get_payload(decode=True)
                                    
                                    # --- חילוץ ועיבוד תאריך ---
                                    email_date_str = msg["Date"]
                                    try:
                                        dt_obj = parsedate_to_datetime(email_date_str)
                                        # המרה לפורמט יפה: DD/MM/YY
                                        formatted_date = dt_obj.strftime("%d/%m/%y")
                                    except:
                                        formatted_date = datetime.now().strftime("%d/%m/%y")
                                        
                                    status_container.success(f"✅ מלאי עודכן בהצלחה (תאריך: {formatted_date})")
                                    
                                    mail.close()
                                    mail.logout()
                                    
                                    try:
                                        excel_file = io.BytesIO(file_data)
                                        df_temp = pd.read_excel(excel_file, header=None)
                                        header_row = -1
                                        for i, row in df_temp.iterrows():
                                            if "פריט" in row.astype(str).values:
                                                header_row = i
                                                break
                                        
                                        if header_row == -1: return None, None
                                        
                                        excel_file.seek(0)
                                        df_inv = pd.read_excel(excel_file, header=header_row)
                                        
                                        df_inv["כמות זמינה"] = pd.to_numeric(df_inv["כמות זמינה"], errors="coerce").fillna(0).astype(int)
                                        pivot_inv = df_inv.groupby("פריט")["כמות זמינה"].sum().reset_index()
                                        pivot_inv.columns = [COL_SKU, "מלאי_נוכחי"]
                                        pivot_inv[COL_SKU] = pivot_inv[COL_SKU].apply(clean_sku)
                                        
                                        # שמירת הקובץ
                                        pivot_inv.to_csv(INVENTORY_CACHE_FILE, index=False)
                                        
                                        # שמירת התאריך בקובץ נפרד
                                        with open(INVENTORY_DATE_FILE, "w") as f:
                                            f.write(formatted_date)
                                            
                                        return pivot_inv, formatted_date
                                        
                                    except Exception as e:
                                        st.error(f"שגיאה בעיבוד אקסל: {e}")
                                        return None, None
        
        status_container.warning("לא נמצא קובץ אקסל מתאים במיילים האחרונים.")
        mail.close()
        mail.logout()
        return None, None

    except Exception as e:
        st.error(f"שגיאה בהתחברות למייל: {e}")
        return None, None

# ==========================================
# 🖥️ ממשק ראשי
# ==========================================

# טעינת נתונים (רגילים + pre_orders)
df, df_pre_orders = load_data_from_sql()

# --- סרגל צד ---
st.sidebar.title("תפריט")

if st.sidebar.button("🔄 רענן נתונים עכשיו"):
    load_data_from_sql.clear()
    st.rerun()

st.sidebar.divider()

# טעינה ראשונית של המלאי והתאריך
if "inventory_df" not in st.session_state:
    cached_inv, cached_date = load_inventory_cache()
    if cached_inv is not None:
        st.session_state["inventory_df"] = cached_inv
        st.session_state["inventory_date"] = cached_date
    else:
        st.session_state["inventory_df"] = None
        st.session_state["inventory_date"] = None

# הכפתור עם הטקסט המעודכן שלך
if st.sidebar.button("📧 משוך מלאי עדכני"):
    inv_data, inv_date_str = fetch_inventory_from_email()
    if inv_data is not None:
        st.session_state["inventory_df"] = inv_data
        st.session_state["inventory_date"] = inv_date_str
        # ההודעה עצמה מוצגת כבר בתוך הפונקציה

st.title("📦 דשבורד ניהול הזמנות")

# --- חישוב תחזית מכירות חודשית (KPIs עליונים) ---
try:
    now = datetime.now(ZoneInfo("Asia/Jerusalem"))
except Exception:
    now = datetime.now()

current_month_start = now.replace(day=1).date()
today_date = now.date()

days_in_current_month = calendar.monthrange(now.year, now.month)[1]
current_day_num = now.day

df_curr_month = df[
    (df['date_only'] >= current_month_start) & 
    (df['date_only'] <= today_date)
]
total_packages_mtd = df_curr_month[COL_QUANTITY].sum()

forecast_revenue = 0
forecast_revenue_net = 0

if current_day_num > 0:
    daily_avg = total_packages_mtd / current_day_num
    forecast_packages = daily_avg * days_in_current_month
    forecast_revenue = int(forecast_packages * 390)
    forecast_revenue_net = int(forecast_revenue * 0.95)

# הצגת KPIs עליונים
kpi_top1, kpi_top2, kpi_top3 = st.columns(3)
kpi_top1.metric("📅 יום בחודש", f"{current_day_num}/{days_in_current_month}")
kpi_top2.metric("💰 צפי מכירות (ברוטו)", f"₪{forecast_revenue:,}")
kpi_top3.metric("📉 צפי מכירות (נטו -5%)", f"₪{forecast_revenue_net:,}")

st.markdown("---")

tab_dashboard, tab_inventory = st.tabs(["📊 דשבורד והזמנות", "🏭 ניתוח מלאי"])

# ========================================================
# TAB 1: דשבורד הזמנות
# ========================================================
with tab_dashboard:
    df_filtered = df.copy()

    with st.container():
        st.markdown("### 📅 סינון לפי תאריכים")
        
        today_default = datetime.now(ZoneInfo("Asia/Jerusalem")).date()
        first_of_month = today_default.replace(day=1)
        
        col_filter1, col_filter2, col_spacer = st.columns([1, 1, 2])
        
        with col_filter1:
            start_date = st.date_input("מתאריך:", value=first_of_month, format="DD/MM/YYYY")
        with col_filter2:
            end_date = st.date_input("עד תאריך:", value=today_default, format="DD/MM/YYYY")

        if start_date and end_date:
            if start_date <= end_date:
                mask_date = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
                df_filtered = df_filtered.loc[mask_date]
            else:
                st.error("⚠️ תאריך התחלה מאוחר מתאריך סיום")

    st.sidebar.header("🔎 חיפוש מתקדם")
    st.sidebar.info("החיפוש מתבצע בתוך טווח התאריכים שנבחר")
    
    search_options = {
        "מק\"ט": COL_SKU,
        "מספר הזמנה": COL_ORDER_NUM,
        "שם לקוח": COL_CUSTOMER,
        "טלפון": COL_PHONE
    }
    
    search_type_label = st.sidebar.selectbox("חפש לפי:", list(search_options.keys()))
    selected_col = search_options[search_type_label]
    
    search_term = st.sidebar.text_input("ערך לחיפוש:")

    if search_term:
        if selected_col == COL_SKU:
            search_term_norm = clean_sku(search_term)
            st.sidebar.caption(f"🔎 מחפש: {search_term_norm}")
            mask = df_filtered[COL_SKU].str.contains(search_term_norm, na=False)
            df_filtered = df_filtered[mask]

        elif selected_col == COL_PHONE:
            clean_input = re.sub(r'\D', '', search_term)
            if clean_input.startswith('0'): clean_input = clean_input[1:] 
            mask = df_filtered[COL_PHONE].astype(str).str.replace(r'\D','', regex=True).str.contains(clean_input, na=False)
            df_filtered = df_filtered[mask]

        elif selected_col in df_filtered.columns:
            mask = df_filtered[selected_col].astype(str).str.contains(search_term, case=False, na=False)
            df_filtered = df_filtered[mask]

    # --- KPIs ---
    total_rows = len(df_filtered)
    total_packages = int(df_filtered[COL_QUANTITY].sum())
    
    regular_mask = df_filtered[COL_SHIP_NUM].str.strip() != ""
    regular_packages = int(df_filtered.loc[regular_mask, COL_QUANTITY].sum())
    
    install_mask = df_filtered[COL_SHIP_NUM].str.strip() == ""
    install_packages = int(df_filtered.loc[install_mask, COL_QUANTITY].sum())

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("📦 סה\"כ רשומות", total_rows)
    kpi2.metric("🔢 סה\"כ חבילות", f"{total_packages:,}")
    kpi3.metric("🚛 הזמנות רגילות", f"{regular_packages:,}")
    kpi4.metric("🛠️ התקנות", f"{install_packages:,}")
    
    st.markdown("---")

    # --- סטטיסטיקות 3 חודשים + 30 יום ---
    cutoff_90 = datetime.now().date() - timedelta(days=90)
    cutoff_30 = datetime.now().date() - timedelta(days=30)
    
    df_stats_90 = df[df['date_only'] >= cutoff_90].copy()
    df_stats_30 = df[df['date_only'] >= cutoff_30].copy()
    
    if not df_stats_90.empty and COL_SKU in df_stats_90.columns and COL_QUANTITY in df_stats_90.columns:
        
        sku_stats_90 = df_stats_90.groupby(COL_SKU)[COL_QUANTITY].sum().reset_index().rename(columns={COL_QUANTITY: 'sales_90'})
        sku_stats_30 = df_stats_30.groupby(COL_SKU)[COL_QUANTITY].sum().reset_index().rename(columns={COL_QUANTITY: 'sales_30'})
        
        # איחוד הסטטיסטיקות
        sku_stats = pd.merge(sku_stats_90, sku_stats_30, on=COL_SKU, how='left').fillna(0)
        sku_stats['sales_30'] = sku_stats['sales_30'].astype(int)
        
        if not sku_stats.empty:
            st.info("📊 הנתונים בטבלאות למטה מתייחסים ל-3 החודשים האחרונים (ללא קשר לטווח התאריכים שנבחר)")
            
            col_top, col_bottom = st.columns(2)
            
            with col_top:
                st.subheader("🏆 המוצרים המובילים (3 חודשים)")
                top_n = st.number_input("כמות להצגה (ברירת מחדל 10):", min_value=1, value=10, step=1)
                top_df = sku_stats.sort_values(by='sales_90', ascending=False).head(top_n).copy()
                
                # חישוב ממוצע ביקוש
                top_df['avg_monthly_sales'] = (top_df['sales_90'] / 3).astype(int)
                
                top_df = top_df.rename(columns={COL_SKU: 'מק"ט', 'sales_90': 'חבילות (90 יום)', 'sales_30': 'ביקוש (30 יום)'})
                
                st.dataframe(
                    top_df, 
                    hide_index=True, 
                    use_container_width=True,
                    column_config={
                        "avg_monthly_sales": st.column_config.NumberColumn("ממוצע ביקוש (3 חודשים)", format="%d")
                    }
                )

            with col_bottom:
                st.subheader("🐢 מוצרים איטיים / חלשים")
                threshold = st.number_input("הצג מוצרים עם כמות חבילות עד (כולל):", min_value=1, value=3, step=1)
                slow_movers = sku_stats[sku_stats['sales_90'] <= threshold].sort_values(by='sales_90', ascending=True).copy()
                
                slow_movers['avg_monthly_sales'] = (slow_movers['sales_90'] / 3).astype(int)
                
                # --- שינוי כותרות ---
                slow_movers = slow_movers.rename(columns={COL_SKU: 'מק"ט', 'sales_90': 'חבילות (90 יום)', 'sales_30': 'ביקוש (30 יום)'})
                
                st.dataframe(
                    slow_movers, 
                    hide_index=True, 
                    use_container_width=True, 
                    height=300,
                    column_config={
                        "avg_monthly_sales": st.column_config.NumberColumn("ממוצע ביקוש (3 חודשים)", format="%d")
                    }
                )
                st.caption(f"נמצאו {len(slow_movers)} מוצרים")

    st.markdown("---")

    st.subheader("📈 פעילות יומית (בטווח הנבחר)")
    if 'date_only' in df_filtered.columns and not df_filtered.empty:
        daily_data = df_filtered.groupby('date_only').agg({COL_QUANTITY: 'sum', COL_SKU: 'count'}).rename(columns={COL_QUANTITY: 'חבילות', COL_SKU: 'מספר שורות'})
        tab_g1, tab_g2 = st.tabs(["📝 מספר הזמנות", "📊 כמות חבילות"])
        with tab_g1: st.line_chart(daily_data['מספר שורות'], color="#E74C3C") 
        with tab_g2: st.bar_chart(daily_data['חבילות'], color="#2E86C1") 
            
    st.markdown("---")
    st.subheader(f"רשימת הזמנות מלאה ({len(df_filtered)})")
    
    display_cols = [COL_DATE, COL_ORDER_NUM, COL_CUSTOMER, COL_PHONE, COL_CITY, COL_STREET, COL_HOUSE, COL_SKU, COL_QUANTITY, COL_SHIP_NUM]
    final_cols = [c for c in display_cols if c in df_filtered.columns]
    display_df = df_filtered[final_cols].copy()
    if COL_DATE in display_df.columns: display_df[COL_DATE] = display_df[COL_DATE].dt.strftime('%d/%m/%Y')

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)

# ========================================================
# TAB 2: ניתוח מלאי חכם
# ========================================================
with tab_inventory:
    if st.session_state["inventory_df"] is None:
        st.info("💡 אין נתוני מלאי שמורים. לחץ על '📧 משוך מלאי עדכני' בסרגל הצד.")
    else:
        df_inv = st.session_state["inventory_df"].copy()
        
        # שליפת תאריך המלאי מה-Session
        inv_date_display = st.session_state.get("inventory_date", "לא ידוע")
        
        # --- הכנת הנתונים ---
        cutoff_90 = datetime.now().date() - timedelta(days=90)
        cutoff_30 = datetime.now().date() - timedelta(days=30)
        
        sales_90 = df[df['date_only'] >= cutoff_90].groupby(COL_SKU)[COL_QUANTITY].sum().reset_index().rename(columns={COL_QUANTITY: "sales_90"})
        sales_30 = df[df['date_only'] >= cutoff_30].groupby(COL_SKU)[COL_QUANTITY].sum().reset_index().rename(columns={COL_QUANTITY: "sales_30"})
        
        # מיזוג
        merged = pd.merge(df_inv, sales_90, on=COL_SKU, how="left")
        merged = pd.merge(merged, sales_30, on=COL_SKU, how="left")
        
        merged["sales_90"] = merged["sales_90"].fillna(0).astype(int)
        merged["sales_30"] = merged["sales_30"].fillna(0).astype(int)
        merged["מלאי_נוכחי"] = merged["מלאי_נוכחי"].fillna(0).astype(int)
        
        # חישובים לוגיים
        merged["velocity_daily"] = merged["sales_30"] / 30
        merged["avg_monthly_sales"] = (merged["sales_90"] / 3).astype(int)
        
        merged["days_of_inventory"] = merged.apply(
            lambda x: x["מלאי_נוכחי"] / x["velocity_daily"] if x["velocity_daily"] > 0 else 9999, 
            axis=1
        )

        # --- כותרת דינמית עם התאריך ---
        st.subheader(f"🏭 ניתוח מלאי מפוצל (מציג מלאי מתאריך: {inv_date_display})")
        
        # --- שורה עליונה ---
        row1_col1, row1_col2 = st.columns(2)
        
        # טבלה 1: יחידות אחרונות
        with row1_col1:
            st.markdown("#### 📦 יחידות אחרונות")
            threshold_units = st.number_input("הצג מוצרים עם מלאי פיזי מתחת ל:", min_value=0, value=10, step=1, key="th_units")
            
            df_last_units = merged[merged["מלאי_נוכחי"] < threshold_units].sort_values("מלאי_נוכחי", ascending=True)
            
            st.dataframe(
                df_last_units[[COL_SKU, "מלאי_נוכחי", "avg_monthly_sales", "sales_30"]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "מלאי_נוכחי": st.column_config.NumberColumn("יחידות במלאי", format="%d"),
                    "avg_monthly_sales": st.column_config.NumberColumn("ממוצע ביקוש (3 חודשים)", format="%d"),
                    "sales_30": st.column_config.NumberColumn("ביקוש (30 יום)", format="%d")
                }
            )
            st.caption(f"נמצאו {len(df_last_units)} מוצרים")

        # טבלה 2: ימי מלאי נמוכים
        with row1_col2:
            st.markdown("#### ⏳ ימי מלאי נמוכים")
            threshold_days = st.number_input("הצג מוצרים עם ימי מלאי מתחת ל:", min_value=0, value=31, step=1, key="th_days")
            
            df_low_days = merged[
                (merged["days_of_inventory"] < threshold_days) & 
                (merged["מלאי_נוכחי"] > 0)
            ].sort_values("days_of_inventory", ascending=True)
            
            display_low_days = df_low_days.copy()
            display_low_days["days_of_inventory"] = display_low_days["days_of_inventory"].astype(int)
            
            st.dataframe(
                display_low_days[[COL_SKU, "מלאי_נוכחי", "avg_monthly_sales", "sales_30", "days_of_inventory"]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "מלאי_נוכחי": st.column_config.NumberColumn("במלאי", format="%d"),
                    "avg_monthly_sales": st.column_config.NumberColumn("ממוצע ביקוש (3 חודשים)", format="%d"),
                    "sales_30": st.column_config.NumberColumn("ביקוש (30 יום)", format="%d"),
                    "days_of_inventory": st.column_config.NumberColumn("ימים לסיום המלאי", format="%d")
                }
            )
            st.caption(f"נמצאו {len(df_low_days)} מוצרים")

        st.divider()

        # --- שורה תחתונה ---
        row2_col1, row2_col2 = st.columns(2)
        
        # טבלה 3: מלאי מת
        with row2_col1:
            st.markdown("#### 💀 מלאי מת / איטי")
            threshold_dead = st.number_input("הצג מוצרים שנמכרו (ב-90 יום) עד:", min_value=0, value=0, step=1, key="th_dead")
            
            df_dead = merged[
                (merged["sales_90"] <= threshold_dead) & 
                (merged["מלאי_נוכחי"] > 0)
            ].sort_values("מלאי_נוכחי", ascending=False)
            
            st.dataframe(
                df_dead[[COL_SKU, "מלאי_נוכחי", "sales_90", "sales_30"]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "מלאי_נוכחי": st.column_config.NumberColumn("תקוע במלאי", format="%d"),
                    "sales_90": st.column_config.NumberColumn("מכירות (90 יום)", format="%d"),
                    "sales_30": st.column_config.NumberColumn("מכירות (30 יום)", format="%d")
                }
            )
            st.caption(f"נמצאו {len(df_dead)} מוצרים")

        # טבלה 4: זמן אספקה ארוך
        with row2_col2:
            st.markdown("#### 🚢 זמן אספקה ארוך (Pre-Order)")
            threshold_pre = st.number_input("הצג מוצרים עם כמות מוזמנת מעל:", min_value=0, value=0, step=1, key="th_pre")
            
            if not df_pre_orders.empty:
                df_pre_filtered = df_pre_orders[df_pre_orders['backlog_qty'] > threshold_pre].copy()
                
                pre_view = df_pre_filtered.rename(columns={
                    'sku': 'מק"ט',
                    'backlog_qty': 'כמות'
                }).sort_values('כמות', ascending=False)
                
                st.dataframe(
                    pre_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "כמות": st.column_config.NumberColumn("הוזמן ע\"י לקוחות", format="%d"),
                    }
                )
                st.caption(f"נמצאו {len(pre_view)} מוצרים")
            else:
                st.info("אין נתונים להצגה")
