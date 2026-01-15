import streamlit as st
import pandas as pd
import psycopg2
import re
import imaplib
import email
from email.header import decode_header
import io
import os
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

# ==========================================
# 🎨 CSS
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
    
    /* עיצוב טבלאות - הסתרת אינדקס */
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
        query = """
            SELECT order_num, customer_name, phone, city, street, house_num, sku, quantity, shipping_num, order_date 
            FROM orders
        """
        df = pd.read_sql(query, conn)
        conn.close()

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
            df[COL_QUANTITY] = pd.to_numeric(df[COL_QUANTITY], errors='coerce').fillna(0)

        if COL_SKU in df.columns:
            df[COL_SKU] = df[COL_SKU].apply(clean_sku)

        return df

    except Exception as e:
        st.error(f"שגיאה בחיבור למסד הנתונים: {e}")
        return pd.DataFrame()

def load_inventory_cache():
    """טעינת מלאי מקובץ מקומי אם קיים"""
    if os.path.exists(INVENTORY_CACHE_FILE):
        try:
            df_inv = pd.read_csv(INVENTORY_CACHE_FILE)
            # וידוא נרמול בטעינה מהקובץ
            if COL_SKU in df_inv.columns:
                df_inv[COL_SKU] = df_inv[COL_SKU].apply(clean_sku)
            return df_inv
        except Exception:
            return None
    return None

def fetch_inventory_from_email():
    """משיכת קובץ המלאי האחרון מהמייל ושמירה למטמון"""
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

        status, messages = mail.search(None, f'FROM "{TARGET_SENDER}"')
        if not messages[0]:
            status_container.warning(f"לא נמצאו מיילים מ-{TARGET_SENDER}")
            return None

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
                                    status_container.success(f"✅ נמצא קובץ: {filename} בתאריך {msg['Date']}")
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
                                        
                                        if header_row == -1: return None
                                        
                                        excel_file.seek(0)
                                        df_inv = pd.read_excel(excel_file, header=header_row)
                                        
                                        df_inv["כמות זמינה"] = pd.to_numeric(df_inv["כמות זמינה"], errors="coerce").fillna(0)
                                        pivot_inv = df_inv.groupby("פריט")["כמות זמינה"].sum().reset_index()
                                        pivot_inv.columns = [COL_SKU, "מלאי_נוכחי"]
                                        pivot_inv[COL_SKU] = pivot_inv[COL_SKU].apply(clean_sku)
                                        
                                        # שמירה למטמון
                                        pivot_inv.to_csv(INVENTORY_CACHE_FILE, index=False)
                                        
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
# 🖥️ ממשק ראשי
# ==========================================

# טעינת נתונים ראשונית
df = load_data_from_sql()

# --- סרגל צד ---
st.sidebar.title("תפריט")

if st.sidebar.button("🔄 רענן נתונים עכשיו"):
    load_data_from_sql.clear()
    st.rerun()

st.sidebar.divider()

# ניהול מלאי - טעינה ראשונית מהמטמון אם קיים
if "inventory_df" not in st.session_state:
    cached_inv = load_inventory_cache()
    if cached_inv is not None:
        st.session_state["inventory_df"] = cached_inv
    else:
        st.session_state["inventory_df"] = None

# כפתור משיכה יזומה
if st.sidebar.button("📧 משוך מלאי מהמייל"):
    inv_data = fetch_inventory_from_email()
    if inv_data is not None:
        st.session_state["inventory_df"] = inv_data
        st.sidebar.success("המלאי עודכן ונשמר!")

st.title("📦 דשבורד ניהול הזמנות")

# --- לשוניות (Tabs) לשמירה על סדר ---
tab_dashboard, tab_inventory = st.tabs(["📊 דשבורד והזמנות", "🏭 ניתוח מלאי"])

# ========================================================
# TAB 1: דשבורד הזמנות
# ========================================================
with tab_dashboard:
    df_filtered = df.copy()

    with st.container():
        st.markdown("### 📅 סינון לפי תאריכים")
        
        # --- תיקון: ברירת מחדל מה-1 לחודש הנוכחי עד היום ---
        today = datetime.now().date()
        first_of_month = today.replace(day=1)
        
        col_filter1, col_filter2, col_spacer = st.columns([1, 1, 2])
        
        with col_filter1:
            start_date = st.date_input("מתאריך:", value=first_of_month, format="DD/MM/YYYY")
        with col_filter2:
            end_date = st.date_input("עד תאריך:", value=today, format="DD/MM/YYYY")

        if start_date and end_date:
            if start_date <= end_date:
                mask_date = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
                df_filtered = df_filtered.loc[mask_date]
            else:
                st.error("⚠️ תאריך התחלה מאוחר מתאריך סיום")

    # --- חיפוש מתקדם (על הטווח המסונן) ---
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

    # --- KPIs (מבוסס על הטווח המסונן) ---
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

    # =======================================================
    # גרפים וסטטיסטיקות (לוגיקה נפרדת: 3 חודשים אחרונים)
    # =======================================================
    
    # חישוב Dataset נפרד ל-3 חודשים אחרונים (ללא קשר לפילטר למעלה)
    cutoff_stats = datetime.now().date() - timedelta(days=90)
    df_stats_3m = df[df['date_only'] >= cutoff_stats].copy()
    
    if not df_stats_3m.empty and COL_SKU in df_stats_3m.columns and COL_QUANTITY in df_stats_3m.columns:
        
        sku_stats = df_stats_3m.groupby(COL_SKU)[COL_QUANTITY].sum().reset_index()
        total_q_stats = df_stats_3m[COL_QUANTITY].sum()
        
        if not sku_stats.empty:
            # הסרתי את המטריקה של "המק"ט הכי נמכר"
            
            st.info("📊 הנתונים בטבלאות למטה מתייחסים ל-3 החודשים האחרונים (ללא קשר לטווח התאריכים שנבחר למעלה)")
            
            col_top, col_bottom = st.columns(2)
            
            # --- עמודה ימנית: המוצרים המובילים (ב-3 חודשים) ---
            with col_top:
                st.subheader("🏆 המוצרים המובילים (3 חודשים)")
                
                top_n = st.number_input(
                    "כמות להצגה (ברירת מחדל 10):", 
                    min_value=1, 
                    value=10, 
                    step=1
                )
                
                top_df = sku_stats.sort_values(by=COL_QUANTITY, ascending=False).head(top_n).copy()
                if total_q_stats > 0:
                    top_df['נתח שוק (%)'] = (top_df[COL_QUANTITY] / total_q_stats * 100).round(1).astype(str) + '%'
                top_df = top_df.rename(columns={COL_SKU: 'מק"ט', COL_QUANTITY: 'חבילות'})
                st.dataframe(top_df, hide_index=True, use_container_width=True)

            # --- עמודה שמאלית: מוצרים איטיים (ב-3 חודשים) ---
            with col_bottom:
                st.subheader("🐢 מוצרים איטיים / חלשים")
                
                threshold = st.number_input(
                    "הצג מוצרים עם כמות חבילות עד (כולל):", 
                    min_value=1, 
                    value=3, 
                    step=1
                )
                
                slow_movers = sku_stats[sku_stats[COL_QUANTITY] <= threshold].sort_values(by=COL_QUANTITY, ascending=True).copy()
                
                if total_q_stats > 0:
                    slow_movers['נתח שוק (%)'] = (slow_movers[COL_QUANTITY] / total_q_stats * 100).round(1).astype(str) + '%'
                
                slow_movers = slow_movers.rename(columns={COL_SKU: 'מק"ט', COL_QUANTITY: 'חבילות'})
                
                st.dataframe(slow_movers, hide_index=True, use_container_width=True, height=300)
                st.caption(f"נמצאו {len(slow_movers)} מוצרים")

    st.markdown("---")

    # --- גרף פעילות יומית (חוזרים לטווח המסונן של המשתמש) ---
    st.subheader("📈 פעילות יומית (בטווח הנבחר)")
    if 'date_only' in df_filtered.columns and not df_filtered.empty:
        daily_data = df_filtered.groupby('date_only').agg({
            COL_QUANTITY: 'sum',  
            COL_SKU: 'count'
        }).rename(columns={COL_QUANTITY: 'חבילות', COL_SKU: 'מספר שורות'})
        
        tab_g1, tab_g2 = st.tabs(["📝 מספר הזמנות", "📊 כמות חבילות"])
        
        with tab_g1:
            st.line_chart(daily_data['מספר שורות'], color="#E74C3C") 

        with tab_g2:
            st.bar_chart(daily_data['חבילות'], color="#2E86C1") 
            
    # --- טבלה מלאה ---
    st.markdown("---")
    st.subheader(f"רשימת הזמנות מלאה ({len(df_filtered)})")
    
    display_cols = [COL_DATE, COL_ORDER_NUM, COL_CUSTOMER, COL_PHONE, COL_CITY, COL_STREET, COL_HOUSE, COL_SKU, COL_QUANTITY, COL_SHIP_NUM]
    final_cols = [c for c in display_cols if c in df_filtered.columns]
    
    display_df = df_filtered[final_cols].copy()
    
    if COL_DATE in display_df.columns:
        display_df[COL_DATE] = display_df[COL_DATE].dt.strftime('%d/%m/%Y')

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)

# ========================================================
# TAB 2: ניתוח מלאי (מלאי מת / נמוך)
# ========================================================
with tab_inventory:
    if st.session_state["inventory_df"] is None:
        st.info("💡 כדי לראות נתוני מלאי, לחץ על הכפתור '📧 משוך מלאי מהמייל' בסרגל הצד.")
    else:
        df_inv = st.session_state["inventory_df"].copy()
        
        st.subheader("🕵️ ניתוח מלאי חכם")
        st.caption("השוואה בין המלאי הנוכחי (מהמייל האחרון) לבין מכירות ב-90 הימים האחרונים")
        
        # 1. חישוב מכירות ב-90 יום האחרונים מתוך כלל ההזמנות (ללא קשר לפילטר בדשבורד)
        cutoff_date = datetime.now().date() - timedelta(days=90)
        recent_sales = df[df['date_only'] >= cutoff_date]
        
        # סיכום מכירות לפי מק"ט
        sales_summary = recent_sales.groupby(COL_SKU)[COL_QUANTITY].sum().reset_index()
        sales_summary.columns = [COL_SKU, "נמכר_90_יום"]
        
        # 2. מיזוג (Merge) בין המלאי למכירות
        merged = pd.merge(df_inv, sales_summary, on=COL_SKU, how="left")
        merged["נמכר_90_יום"] = merged["נמכר_90_יום"].fillna(0).astype(int)
        
        # 3. לוגיקה
        dead_stock = merged[(merged["מלאי_נוכחי"] > 0) & (merged["נמכר_90_יום"] == 0)].sort_values("מלאי_נוכחי", ascending=False)
        low_stock = merged[(merged["מלאי_נוכחי"] > 0) & (merged["מלאי_נוכחי"] < 10)].sort_values("מלאי_נוכחי", ascending=True)

        col_dead, col_low = st.columns(2)
        
        with col_dead:
            st.error(f"💀 מלאי מת ({len(dead_stock)} מוצרים)")
            st.caption("מוצרים שקיימים במלאי אך לא נמכרו כלל ב-3 החודשים האחרונים")
            st.dataframe(
                dead_stock[[COL_SKU, "מלאי_נוכחי"]], 
                use_container_width=True, 
                hide_index=True,
                column_config={"מלאי_נוכחי": st.column_config.NumberColumn("יחידות במלאי", format="%d")}
            )
            
        with col_low:
            st.warning(f"⚠️ מלאי נמוך ({len(low_stock)} מוצרים)")
            st.caption("מוצרים עם פחות מ-10 יחידות")
            st.dataframe(
                low_stock[[COL_SKU, "מלאי_נוכחי", "נמכר_90_יום"]], 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "מלאי_נוכחי": st.column_config.NumberColumn("במלאי", format="%d"),
                    "נמכר_90_יום": st.column_config.NumberColumn("מכירות (3 חודשים)", format="%d")
                }
            )
