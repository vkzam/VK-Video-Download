# --- Токены ---
# Токен вашего Telegram бота (полученный от @BotFather)
TELEGRAM_BOT_TOKEN = "6097870665:AAETzFSndlpvpirAJrPE8OTVqN290V2GK78"

# Сервисный ключ доступа VK API (полученный в настройках приложения VK)
VK_SERVICE_TOKEN = "vk1.a.NMDMIvU5eBWBqHcgiwEFBzQVfjyW70iF8GwTlZ81RTLmVZcmrzIWa6lfq3VATXvkHCJ8hgWr-yyXxDfdmb1LGsTBg9vZtJrackjpOJhsvZwkDwbSCHpbY__YSA9x0FjKANnOzONKYAcvyPss0ob0ELbjp4A9eUO3wSui9QgZjANSgs8XoY-mVuFHbEuofempiXb72hC-KKsmn0WZTPETSg"

# --- Идентификаторы Telegram ---
# Ваш ID в Telegram (или ID администратора) для получения уведомлений об ошибках
ADMIN_CHAT_ID = "570263334" 

# ID чата/канала Telegram, куда будут отправляться посты из основной группы VK
TARGET_TELEGRAM_CHAT_ID = "570263334"

# ID чатов для других групп (если нужно) - можно добавлять по аналогии
# OTHER_TARGET_CHAT_ID_1 = "ID_ДРУГОГО_ЧАТА_1"
# OTHER_TARGET_CHAT_ID_2 = "ID_ДРУГОГО_ЧАТА_2"

# --- Идентификаторы групп VK ---
# ID группы VK, посты из которой будут пересылаться в TARGET_TELEGRAM_CHAT_ID
# Указывается только числовой ID (без знака "-")
PRIMARY_VK_GROUP_ID = "66834402"  # Пример: Manacost

# ID других групп VK для мониторинга (можно добавлять)
# Ключ - это короткое имя для файла состояния, значение - ID группы VK
# Посты из этих групп по умолчанию будут отправляться админу (ADMIN_CHAT_ID)
# или вы можете указать другой ID чата в main.py при вызове функции
SECONDARY_VK_GROUPS = {
    #"funny_hs": "98196840",    # Пример: Fanny HS
    #"m3s": "137894175",       # Пример: M3S
    #"my_hs_group": "148007989", # Пример: Ваша группа
    # "olesya": "66868490",     # Пример: Olesya
}

# --- Имена файлов ---
# Файл для хранения ID последних отправленных постов для каждой группы
# Имена будут формироваться как "posts_state_{key}.json", где key - ключ из *_VK_GROUP_ID
POST_STATE_FILE_PREFIX = "posts_state"

# Файл для хранения списка слов-фильтров
FILTER_WORDS_FILE = "filter_words.json"

# Файл для логов
LOG_FILE = "bot.log"
MAX_LOG_SIZE_MB = 10              # Максимальный размер одного лог-файла в МБ
LOG_BACKUP_COUNT = 2              # Количество хранимых архивных лог-файлов

# --- Настройки ---
# Пауза между проверками новых постов в VK (в секундах)
VK_CHECK_INTERVAL_SECONDS = 120

# Максимальное количество постов, запрашиваемых из VK за один раз
VK_POSTS_COUNT = 15 
