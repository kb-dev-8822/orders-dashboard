import streamlit as st
import pandas as pd
import psycopg2
import re
import imaplib
import email
from email.header import decode_header
import io
from datetime import datetime, timedelta

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
# 🎨 CSS עיצוב RTL והעלמת כפתורים מיותרים
# ==========================================
st.markdown("""
<style>
    .stApp { direction: rtl; text-align: right; }
    h1, h2, h3, p, div, .stMarkdown, .stRadio, .stSelectbox, .stTextInput, .stAlert { text-align: right; }
    [data-testid="stMetricValue"] { direction: ltr; text-align: right; }
    [data-testid="stMetricLabel"] { text-align: right; }
    .stButton button { width: 100%; }
    [data-testid="stSidebarCollapsedControl"] { display: none !important; }
    section[data-testid="stSidebar"] > div > div:first-child button { display: none !important; }
    section[data-testid="stSidebar"] { direction: rtl; }
    
    /* עיצוב טבלאות */
    thead tr th:first-child {display:none}
    tbody th {display:none}
</style>
""", unsafe_allow_html=True)

# ==========================================
# 🔐 מנגנון אבטחה (Login)
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
# ⚙️ הגדרות חיבורים ושדות
# ==========================================
try:
    DB_HOST = st.secrets["supabase"]["DB_HOST"]
    DB_PORT = st.secrets["supabase"]["DB_PORT"]
    DB_NAME = st.secrets["supabase"]["DB_NAME"]
    DB_USER = st.secrets["supabase"]["DB_USER"]
    DB_PASS = st.secrets["supabase"]["DB_PASS"]
except:
    st.error("❌ שגיאה: חסרים פרטי חיבור ל-Supabase ב-secrets.toml")
    st.stop()

# עמודות
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
# 🛠️ פונקציות עזר
# ==========================================

def normalize_phone_display(phone_val):
    """
    מוודא שהטלפון מוצג עם 0 מוביל
    """
    if pd.isna(phone_val) or str(phone_val).strip() == "":
        return ""
    
    s = str(phone_val).replace('.0', '').strip()
    clean = re.sub(r'\D', '', s) # משאיר רק מספרים
    
    if not clean:
        return ""
        
    # אם חסר 0 בהתחלה - נוסיף אותו
    if not clean.startswith('0'):
        clean = '0' + clean
        
    return clean

def clean_sku(val):
    """נרמול מקטים להשוואה"""
    if pd.isna(val): return ""
    val = str(val).upper()
    val = val.replace('/', ' ').replace('\\', ' ')
    val = re.sub(r'\s+', ' ', val).strip()
    return val

# ==========================================
# 📥 פונקציות טעינת נתונים
# ==========================================

@st.cache_data(ttl=600)
def load_orders_data():
    """טעינת הזמנות מה-SQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS, sslmode='require'
        )
        query = """
            SELECT order_num, customer_name, phone, city, street, house_num, sku, quantity, shipping_num, order_date 
            FROM orders
        """
        df = pd.read_sql(query, conn)
        conn.close()

        # Rename columns
        df = df.rename(columns={
            'order_num': COL_ORDER_NUM, 'customer_name': COL_CUSTOMER, 'phone': COL_PHONE,
            'city': COL_CITY, 'street': COL_STREET, 'house_num': COL_HOUSE,
            'sku': COL_SKU, 'quantity': COL_QUANTITY, 'shipping_num': COL_SHIP_NUM, 'order_date': COL_DATE
        })

        # Process Date
        df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors='coerce')
        df = df.dropna(subset=[COL_DATE])
        df['date_only'] = df[COL_DATE].dt.date

        # Process Strings & Phone
        df[COL_SKU] = df[COL_SKU].apply(clean_sku)
        
        # --- תיקון: שימוש בפונקציה החדשה שמוסיפה 0 ---
        if COL_PHONE in df.columns:
            df[COL_PHONE] = df[COL_PHONE].apply(normalize_phone_display)
        # ---------------------------------------------

        # Process Quantity
        df[COL_QUANTITY] = pd.to_numeric(df[COL_QUANTITY], errors='coerce').fillna(0)

        return df
    except Exception as e:
        st.error(f"שגיאה בטעינת הזמנות: {e}")
        return pd.DataFrame()

def fetch_inventory_from_email():
    """משיכת קובץ המלאי האחרון מהמייל (ללא שמירה לדיסק)"""
    if "email" not in st.secrets:
        st.error("חסרים פרטי אימייל ב-secrets.toml")
        return None

    EMAIL_USER = st.secrets["email"]["user"]
    EMAIL_PASS = st.secrets["email"]["password"]
    TARGET_SENDER = st.secrets["email"].get("sender_email", "GlobusInfo@globus-intr.co.il")
    TARGET_SUBJECT = "מלאי סלים פרייס"
    FILE_TO_FIND = "stock122.xlsx"

    status_container = st.empty()
    status_container.info("🔄 מתחבר ל-Gmail ומושך קובץ מלאי...")

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")

        # חיפוש לפי שולחן
        status, messages = mail.search(None, f'FROM "{TARGET_SENDER}"')
        if not messages[0]:
            status_container.warning(f"לא נמצאו מיילים מ-{TARGET_SENDER}")
            return None

        email_ids = messages[0].split()
        
        # עוברים על 10 המיילים האחרונים
        for eid in reversed(email_ids[-10:]):
            _, msg_data = mail.fetch(eid, "(RFC822)")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding if encoding else "utf-8")
                    
                    if TARGET_SUBJECT in subject:
                        # נמצא מייל מתאים - מחפשים את הקובץ
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
                                    status_container.success(f"✅ נמצא קובץ: {filename} בתאריך {msg['Date']}")
                                    mail.close()
                                    mail.logout()
                                    
                                    # עיבוד הקובץ בזיכרון
                                    try:
                                        excel_file = io.BytesIO(file_data)
                                        
                                        # מציאת שורת כותרת "פריט"
                                        df_temp = pd.read_excel(excel_file, header=None)
                                        header_row = -1
                                        for i, row in df_temp.iterrows():
                                            if "פריט" in row.astype(str).values:
                                                header_row = i
                                                break
                                        
                                        if header_row == -1: return None
                                        
                                        # טעינת הנתונים האמיתיים
                                        excel_file.seek(0)
                                        df_inv = pd.read_excel(excel_file, header=header_row)
                                        
                                        # PIVOT בזיכרון
                                        df_inv["כמות זמינה"] = pd.to_numeric(df_inv["כמות זמינה"], errors="coerce").fillna(0)
                                        pivot_inv = df_inv.groupby("פריט")["כמות זמינה"].sum().reset_index()
                                        pivot_inv.columns = [COL_SKU, "מלאי_נוכחי"]
                                        pivot_inv[COL_SKU] = pivot_inv[COL_SKU].apply(clean_sku)
                                        
                                        return pivot_inv
                                        
                                    except Exception as e:
                                        st.error(f"שגיאה בעיבוד אקסל: {e}")
                                        return None
        
        status_container.warning("לא נמצא קובץ אקסל מתאים במיילים האחרונים.")
        mail.close()
        mail.logout()
        return None

    except Exception as e:
        st.error(f"שגיאה בהתחברות למייל: {e}")
        return None


# ==========================================
# 🖥️ ממשק משתמש (UI)
# ==========================================

# --- טעינת נתונים ראשונית ---
df_orders = load_orders_data()

# --- סרגל צד ---
st.sidebar.title("📦 תפריט")

# כפתור רענון הזמנות (SQL)
if st.sidebar.button("🔄 רענן נתוני הזמנות"):
    load_orders_data.clear()
    st.rerun()

st.sidebar.divider()

# ניהול מלאי (Session State)
if "inventory_df" not in st.session_state:
    st.session_state["inventory_df"] = None

st.sidebar.header("🏭 ניהול מלאי")
if st.sidebar.button("📧 משוך מלאי עדכני מהמייל"):
    inv_data = fetch_inventory_from_email()
    if inv_data is not None:
        st.session_state["inventory_df"] = inv_data
        st.sidebar.success(f"עודכן: {len(inv_data)} מק\"טים")
    else:
        st.sidebar.error("נכשל במשיכת מלאי")

# --- גוף הדשבורד ---
st.title("📦 דשבורד ניהול הזמנות")

# לשוניות ראשיות
tab_main, tab_inventory = st.tabs(["📊 דשבורד הזמנות", "💀 מלאי מת / נמוך"])

# ==========================================
# TAB 1: דשבורד הזמנות (הקוד המקורי המשופר)
# ==========================================
with tab_main:
    # פילטרים
    st.markdown("### 📅 סינון לפי תאריכים")
    default_end = datetime.now().date()
    default_start = default_end - timedelta(days=30)
    
    if not df_orders.empty:
        d_min = df_orders['date_only'].min()
        d_max = df_orders['date_only'].max()
        if pd.notnull(d_min): default_start = d_min
        if pd.notnull(d_max): default_end = d_max

    c1, c2, _ = st.columns([1, 1, 2])
    start_date = c1.date_input("מ:", value=default_start, format="DD/MM/YYYY")
    end_date = c2.date_input("עד:", value=default_end, format="DD/MM/YYYY")

    # סינון ה-DF
    df_filtered = df_orders.copy()
    if start_date and end_date:
        mask = (df_filtered['date_only'] >= start_date) & (df_filtered['date_only'] <= end_date)
        df_filtered = df_filtered.loc[mask]

    # חיפוש מתקדם
    with st.expander("🔎 חיפוש הזמנה ספציפי"):
        s_col1, s_col2 = st.columns([1, 3])
        search_type = s_col1.selectbox("חפש לפי:", [COL_SKU, COL_ORDER_NUM, COL_CUSTOMER, COL_PHONE])
        search_val = s_col2.text_input("ערך לחיפוש:")
        
        if search_val:
            if search_type == COL_SKU:
                clean_val = clean_sku(search_val)
                df_filtered = df_filtered[df_filtered[COL_SKU].str.contains(clean_val, na=False)]
            elif search_type == COL_PHONE:
                # מנקים את החיפוש ומחפשים בתוך הטקסט הקיים (שיש בו כבר 0 מוביל או לא)
                clean_input = re.sub(r'\D', '', search_val) 
                # הסרת 0 מוביל לצורך השוואה גמישה
                if clean_input.startswith('0'): clean_input = clean_input[1:]
                
                # חיפוש "מכיל" את המספר (כך ש-050 ימצא את 050...)
                df_filtered = df_filtered[df_filtered[COL_PHONE].str.replace(r'\D','', regex=True).str.contains(clean_input, na=False)]
            else:
                df_filtered = df_filtered[df_filtered[search_type].astype(str).str.contains(search_val, case=False, na=False)]

    # KPI
    total_pkgs = int(df_filtered[COL_QUANTITY].sum())
    reg_pkgs = int(df_filtered[df_filtered[COL_SHIP_NUM].str.strip() != ""][COL_QUANTITY].sum())
    install_pkgs = int(df_filtered[df_filtered[COL_SHIP_NUM].str.strip() == ""][COL_QUANTITY].sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("סה\"כ הזמנות", len(df_filtered))
    k2.metric("📦 חבילות", f"{total_pkgs:,}")
    k3.metric("🚛 משלוחים", f"{reg_pkgs:,}")
    k4.metric("🛠️ התקנות", f"{install_pkgs:,}")

    st.divider()

    # טבלה ראשית
    st.subheader("📋 רשימת הזמנות")
    
    # בחירת עמודות לתצוגה
    disp_cols = [COL_DATE, COL_ORDER_NUM, COL_CUSTOMER, COL_PHONE, COL_CITY, COL_SKU, COL_QUANTITY, COL_SHIP_NUM]
    final_disp = df_filtered[[c for c in disp_cols if c in df_filtered.columns]].copy()
    
    # עיצוב תאריך
    if COL_DATE in final_disp.columns:
        final_disp[COL_DATE] = final_disp[COL_DATE].dt.strftime('%d/%m/%Y')

    st.dataframe(final_disp, use_container_width=True, hide_index=True, height=400)


# ==========================================
# TAB 2: ניתוח מלאי (החלק החדש והחכם)
# ==========================================
with tab_inventory:
    if st.session_state["inventory_df"] is None:
        st.info("💡 כדי לראות נתוני מלאי מת, לחץ על הכפתור '📧 משוך מלאי מהמייל' בסרגל הצד.")
    else:
        df_inv = st.session_state["inventory_df"].copy()
        
        st.subheader("🕵️ ניתוח מלאי חכם")
        st.caption("השוואה בין המלאי הנוכחי (מהמייל האחרון) לבין מכירות ב-90 הימים האחרונים")
        
        # 1. חישוב מכירות ב-90 יום האחרונים
        cutoff_date = datetime.now().date() - timedelta(days=90)
        recent_sales = df_orders[df_orders['date_only'] >= cutoff_date]
        
        # סיכום מכירות לפי מק"ט
        sales_summary = recent_sales.groupby(COL_SKU)[COL_QUANTITY].sum().reset_index()
        sales_summary.columns = [COL_SKU, "נמכר_90_יום"]
        
        # 2. מיזוג (Merge) בין המלאי למכירות
        # Left Join: רוצים את כל המלאי, ורק אם יש מכירות נצמיד אותן
        merged = pd.merge(df_inv, sales_summary, on=COL_SKU, how="left")
        merged["נמכר_90_יום"] = merged["נמכר_90_יום"].fillna(0).astype(int)
        
        # 3. לוגיקה
        # מלאי מת: יש במלאי (>0) אבל נמכר 0
        dead_stock = merged[(merged["מלאי_נוכחי"] > 0) & (merged["נמכר_90_יום"] == 0)].sort_values("מלאי_נוכחי", ascending=False)
        
        # מלאי נמוך: יש במלאי פחות מ-10, אבל המלאי לא ריק
        low_stock = merged[(merged["מלאי_נוכחי"] > 0) & (merged["מלאי_נוכחי"] < 10)].sort_values("מלאי_נוכחי", ascending=True)

        # 4. תצוגה
        col_dead, col_low = st.columns(2)
        
        with col_dead:
            st.error(f"💀 מלאי מת ({len(dead_stock)} מוצרים)")
            st.caption("מוצרים שקיימים במלאי אך לא נמכרו כלל ב-3 החודשים האחרונים")
            st.dataframe(
                dead_stock[[COL_SKU, "מלאי_נוכחי"]], 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "מלאי_נוכחי": st.column_config.NumberColumn("יחידות במלאי", format="%d")
                }
            )
            
        with col_low:
            st.warning(f"⚠️ מלאי נמוך ({len(low_stock)} מוצרים)")
            st.caption("מוצרים עם פחות מ-10 יחידות (שווה להזמין סחורה)")
            st.dataframe(
                low_stock[[COL_SKU, "מלאי_נוכחי", "נמכר_90_יום"]], 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "מלאי_נוכחי": st.column_config.NumberColumn("במלאי", format="%d"),
                    "נמכר_90_יום": st.column_config.NumberColumn("מכירות (3 חודשים)", format="%d")
                }
            )
