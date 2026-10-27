package com.smartir.remote.ui.screens

import android.content.Context
import android.net.Uri
import androidx.appcompat.app.AppCompatDelegate
import androidx.core.os.LocaleListCompat
import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.smartir.remote.R
import com.smartir.remote.data.AppJson
import com.smartir.remote.data.db.HistoryDao
import com.smartir.remote.data.db.RemoteEntity
import com.smartir.remote.data.db.ScanHistoryEntity
import com.smartir.remote.data.model.BackupFile
import com.smartir.remote.data.model.IrProfile
import com.smartir.remote.data.repo.*
import com.smartir.remote.domain.*
import com.smartir.remote.ir.IrEncoder
import com.smartir.remote.ir.IrTransmitter
import com.smartir.remote.scan.*
import dagger.hilt.android.lifecycle.HiltViewModel
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import com.smartir.remote.data.db.MacroDao
import com.smartir.remote.data.db.MacroEntity
import com.smartir.remote.data.model.CommandDef
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import javax.inject.Inject

// ------------------------------------------------------------------ Home
@HiltViewModel
class HomeViewModel @Inject constructor(
    remotes: RemoteRepository,
    transmitter: IrTransmitter,
    profiles: ProfileRepository,
    private val store: SettingsStore,
    @ApplicationContext ctx: Context,
) : ViewModel() {
    val categoryCounts = profiles.observeCategoryCounts().stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyMap())
    val seeding = profiles.seeding
    val profileCount = profiles.observeCount().stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), -1)
    val recent = remotes.observeRecent().stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())
    val favorites = remotes.observeAll().map { l -> l.filter { it.favorite } }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())
    val showWelcome = store.onboarded.map { !it }.stateIn(viewModelScope, SharingStarted.Eagerly, false)
    val hasIr: Boolean = transmitter.isAvailable

    init {
        viewModelScope.launch { profiles.ensureSeeded() }
        // keep the launcher's long-press shortcuts in sync with favourite / recent remotes
        viewModelScope.launch { remotes.observeAll().collect { com.smartir.remote.ui.Shortcuts.update(ctx, it) } }
    }

    fun dismissWelcome() { viewModelScope.launch { store.setOnboarded() } }
}

// ------------------------------------------------------------------ Remote
data class TimerState(val cmd: String, val endAtMs: Long)

data class RemoteState(
    val remote: RemoteEntity? = null,
    val category: DeviceCategory = DeviceCategory.OTHER,
    val available: Set<String> = emptySet(),
    val editing: Boolean = false,
)

@HiltViewModel
class RemoteViewModel @Inject constructor(
    saved: SavedStateHandle,
    private val remotes: RemoteRepository,
    private val profiles: ProfileRepository,
    private val transmitter: IrTransmitter,
    settings: SettingsStore,
    private val macroDao: MacroDao,
) : ViewModel() {
    private val id: Long = checkNotNull(saved.get<Long>("id"))
    private val cache = HashMap<String, IrProfile>()
    private val _state = MutableStateFlow(RemoteState())
    val state = _state.asStateFlow()
    val messages = MutableSharedFlow<String>(extraBufferCapacity = 4)
    val prefs = settings.settings.stateIn(viewModelScope, SharingStarted.Eagerly, AppSettings())
    val macros = macroDao.observe(id).stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())
    val timer = MutableStateFlow<TimerState?>(null)
    val runningMacro = MutableStateFlow<String?>(null)

    private val sendLock = Mutex()
    private var timerJob: Job? = null
    private var macroJob: Job? = null
    private var lastTouch = 0L

    init {
        viewModelScope.launch {
            remotes.observe(id).filterNotNull().collect { r ->
                val ov = r.overrideMap()
                for (pid in ov.values + r.profileId) {
                    if (pid !in cache) profiles.getById(pid)?.let { cache[pid] = it }
                }
                val main = cache[r.profileId]
                val avail = buildSet {
                    main?.let { addAll(it.commands.keys) }
                    ov.forEach { (cmd, pid) -> if (cache[pid]?.commands?.containsKey(cmd) == true) add(cmd) }
                }
                _state.update {
                    it.copy(remote = r, available = avail,
                        category = runCatching { DeviceCategory.valueOf(r.category) }.getOrDefault(DeviceCategory.OTHER))
                }
            }
        }
    }

    private suspend fun transmitProfile(p: IrProfile, cmd: String, wait: Boolean): Boolean {
        val def: CommandDef = p.commands[cmd] ?: return false
        if (wait) sendLock.lock() else if (!sendLock.tryLock()) return false
        try {
            val signal = IrEncoder.encode(p, def)
            withContext(Dispatchers.Default) { transmitter.transmit(signal) }
            val now = System.currentTimeMillis()
            val rid = _state.value.remote?.id
            if (rid != null && now - lastTouch > 5000) { lastTouch = now; remotes.touch(rid) }
            delay(signal.durationMs)
            return true
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            messages.tryEmit(e.message ?: "IR error")
            return false
        } finally {
            sendLock.unlock()
        }
    }

    /**
     * Sends one command. With wait=false a busy transmitter drops the request, so holding a
     * key (auto-repeat) can never queue overlapping signals.
     */
    private suspend fun sendKey(cmd: String, wait: Boolean): Boolean {
        val r = _state.value.remote ?: return false
        val p = cache[r.overrideMap()[cmd] ?: r.profileId] ?: return false
        return transmitProfile(p, cmd, wait)
    }

    fun send(cmd: String) { viewModelScope.launch { sendKey(cmd, false) } }

    fun setEditing(v: Boolean) = _state.update { it.copy(editing = v) }

    fun toggleHidden(cmd: String) {
        val r = _state.value.remote ?: return
        val h = r.hiddenSet().toMutableSet()
        if (!h.add(cmd)) h.remove(cmd)
        viewModelScope.launch { remotes.update(r.copy(hidden = h.joinToString(","))) }
    }

    fun toggleFavorite() {
        val r = _state.value.remote ?: return
        viewModelScope.launch { remotes.setFavorite(r.id, !r.favorite) }
    }

    fun rename(name: String) {
        val r = _state.value.remote ?: return
        viewModelScope.launch { remotes.update(r.copy(name = name.trim().ifBlank { r.name })) }
    }

    /** JSON for "Share remote": this remote plus every profile it uses (importable via paste or restore). */
    suspend fun exportText(): String? {
        val r = _state.value.remote ?: return null
        val ids = setOf(r.profileId) + r.overrideMap().values
        val used = ids.mapNotNull { profiles.getById(it) }
        return AppJson.encodeToString(BackupFile(remotes = listOf(r.copy(id = 0, lastUsed = 0, favorite = false)), userProfiles = used))
    }

    // ---- fix a misbehaving button: try other codes, keep the one that works
    data class FixState(val cmd: String, val total: Int = -1, val index: Int = -1, val alt: IrProfile? = null)

    val fix = MutableStateFlow<FixState?>(null)
    private var fixCandidates: List<IrProfile> = emptyList()

    private fun sigKey(p: IrProfile, d: CommandDef): Triple<Int, Int, Int>? = runCatching {
        val s = IrEncoder.encode(p, d)
        Triple(s.frequency, s.pattern.size, s.pattern.contentHashCode())
    }.getOrNull()

    private fun alternativesFor(main: IrProfile, pool: List<IrProfile>, cmd: String): List<IrProfile> {
        val seen = HashSet<Triple<Int, Int, Int>>()
        main.commands[cmd]?.let { d -> sigKey(main, d)?.let { seen.add(it) } }
        return pool.asSequence()
            .filter { it.id != main.id && it.commands[cmd] != null }
            .sortedWith(compareBy<IrProfile>({ if (it.brand.equals(main.brand, true)) 0 else 1 }, { it.brand }, { it.id }))
            .filter { p -> val k = sigKey(p, p.commands.getValue(cmd)); k != null && seen.add(k) }
            .take(150)
            .toList()
    }

    fun startFix(cmd: String) {
        fix.value = FixState(cmd)
        viewModelScope.launch {
            val r = _state.value.remote ?: return@launch
            val main = cache[r.profileId] ?: return@launch
            val cats = _state.value.category.scanCategories(true)
            val pool = profiles.getForCategories(cats)
            fixCandidates = withContext(Dispatchers.Default) { alternativesFor(main, pool, cmd) }
            fix.update { it?.copy(total = fixCandidates.size) }
        }
    }

    fun nextFix() {
        val f = fix.value ?: return
        if (fixCandidates.isEmpty()) return
        val i = (f.index + 1) % fixCandidates.size
        val alt = fixCandidates[i]
        fix.value = f.copy(index = i, alt = alt)
        viewModelScope.launch { transmitProfile(alt, f.cmd, false) }
    }

    fun acceptFix() {
        val f = fix.value ?: return
        val alt = f.alt ?: return
        val r = _state.value.remote ?: return
        val ov = r.overrideMap().toMutableMap()
        ov[f.cmd] = alt.id
        cache[alt.id] = alt
        viewModelScope.launch { remotes.update(r.copy(overrides = RemoteEntity.encodeOverrides(ov))) }
        fix.value = null
    }

    fun resetKey(cmd: String) {
        val r = _state.value.remote ?: return
        val ov = r.overrideMap().toMutableMap()
        if (ov.remove(cmd) != null) viewModelScope.launch { remotes.update(r.copy(overrides = RemoteEntity.encodeOverrides(ov))) }
        fix.value = null
    }

    fun closeFix() { fix.value = null }

    // ---- macros: ordered button sequences with a fixed delay between steps
    fun runMacro(m: MacroEntity) {
        if (macroJob?.isActive == true) return
        macroJob = viewModelScope.launch {
            runningMacro.value = m.name
            try {
                for (k in m.steps.split(',').filter { it.isNotBlank() }) {
                    sendKey(k, true)
                    delay(m.delayMs)
                }
            } finally { runningMacro.value = null }
        }
    }

    fun cancelMacro() { macroJob?.cancel() }

    fun saveMacro(name: String, steps: List<String>, delayMs: Long) {
        viewModelScope.launch { macroDao.insert(MacroEntity(remoteId = id, name = name.trim(), steps = steps.joinToString(","), delayMs = delayMs)) }
    }

    fun deleteMacro(m: MacroEntity) { viewModelScope.launch { macroDao.delete(m.id) } }

    // ---- timer: sends a command later (works while the app is open)
    fun startTimer(cmd: String, minutes: Int) {
        timerJob?.cancel()
        val ms = minutes * 60_000L
        timer.value = TimerState(cmd, System.currentTimeMillis() + ms)
        timerJob = viewModelScope.launch {
            delay(ms)
            timer.value = null
            sendKey(cmd, true)
        }
    }

    fun cancelTimer() { timerJob?.cancel(); timer.value = null }
}

// ------------------------------------------------------------------ Settings / history
@HiltViewModel
class SettingsViewModel @Inject constructor(
    private val store: SettingsStore,
    private val profiles: ProfileRepository,
    private val remotes: RemoteRepository,
    private val downloader: IrdbDownloader,
    @ApplicationContext private val ctx: Context,
) : ViewModel() {
    val stats = combine(profiles.observeCount(), profiles.observeBrandCount()) { a, b -> a to b }
        .stateIn(viewModelScope, SharingStarted.Eagerly, 0 to 0)
    val downloading: StateFlow<String?> = downloader.progress
        .map { p -> p?.let { ctx.getString(R.string.download_progress_src, it.source.ifBlank { "…" }, it.count) } }
        .stateIn(viewModelScope, SharingStarted.Eagerly, null)

    fun downloadDatabase() = downloader.start()

    init {
        viewModelScope.launch {
            downloader.results.collect { r ->
                if (r.error != null) messages.emit(ctx.getString(R.string.msg_download_failed, r.error))
                messages.emit(ctx.getString(R.string.msg_download_done, r.imported))
                r.note?.let { messages.emit(ctx.getString(R.string.msg_unsupported, it)) }
            }
        }
    }

    val settings = store.settings.stateIn(viewModelScope, SharingStarted.Eagerly, AppSettings())
    val messages = MutableSharedFlow<String>(extraBufferCapacity = 4)

    fun language(): String = AppCompatDelegate.getApplicationLocales().toLanguageTags().substringBefore('-')
    fun setLanguage(tag: String) = AppCompatDelegate.setApplicationLocales(LocaleListCompat.forLanguageTags(tag))
    fun setTheme(v: Int) = viewModelScope.launch { store.setTheme(v) }
    fun setHaptic(v: Boolean) = viewModelScope.launch { store.setHaptic(v) }
    fun setVolumeKeys(v: Boolean) = viewModelScope.launch { store.setVolumeKeys(v) }
    fun setKeepOn(v: Boolean) = viewModelScope.launch { store.setKeepOn(v) }

    private suspend fun readText(uri: Uri): String = withContext(Dispatchers.IO) {
        ctx.contentResolver.openInputStream(uri)?.bufferedReader()?.use { it.readText() } ?: error("cannot open file")
    }

    private suspend fun writeText(uri: Uri, text: String) = withContext(Dispatchers.IO) {
        ctx.contentResolver.openOutputStream(uri, "wt")?.bufferedWriter()?.use { it.write(text) } ?: error("cannot write file")
    }

    fun importProfiles(uris: List<Uri>) = viewModelScope.launch {
        var imported = 0; var skipped = 0
        for (uri in uris) {
            try {
                val r = profiles.importStream(open = { ctx.contentResolver.openInputStream(uri) })
                if (r.error != null) messages.emit(ctx.getString(R.string.msg_import_failed, r.error))
                imported += r.imported; skipped += r.skipped
            } catch (e: Exception) { messages.emit(ctx.getString(R.string.msg_import_failed, e.message ?: "")) }
        }
        messages.emit(ctx.getString(R.string.msg_imported, imported))
        if (skipped > 0) messages.emit(ctx.getString(R.string.msg_skipped, skipped))
    }

    /** Exports the whole profile database (streamed, one profile per line). */
    fun exportProfiles(uri: Uri) = viewModelScope.launch {
        try {
            val n = withContext(Dispatchers.IO) {
                val w = ctx.contentResolver.openOutputStream(uri, "wt")?.bufferedWriter() ?: error("cannot write file")
                w.use { profiles.exportAll(it) }
            }
            messages.emit(ctx.getString(R.string.msg_exported, n))
        } catch (e: Exception) { messages.emit(e.message ?: "error") }
    }

    /** Saved remotes + every profile they use (so the backup also works on a phone without the database). */
    fun backup(uri: Uri) = viewModelScope.launch {
        try {
            val rs = remotes.getAll()
            val ids = rs.flatMap { listOf(it.profileId) + it.overrideMap().values }.toSet()
            val used = ids.mapNotNull { profiles.getById(it) }
            val file = BackupFile(remotes = rs, userProfiles = (profiles.userProfiles() + used).distinctBy { it.id })
            withContext(Dispatchers.IO) {
                ctx.contentResolver.openOutputStream(uri, "wt")?.bufferedWriter()?.use { it.write(AppJson.encodeToString(file)) }
                    ?: error("cannot write file")
            }
            messages.emit(ctx.getString(R.string.msg_done))
        } catch (e: Exception) { messages.emit(e.message ?: "error") }
    }

    private suspend fun restoreText(text: String) {
        val file = AppJson.decodeFromString<BackupFile>(text)
        if (file.remotes.isEmpty() && file.userProfiles.isEmpty()) {
            messages.emit(ctx.getString(R.string.msg_import_failed, "not a backup file"))
            return
        }
        profiles.insertProfiles(file.userProfiles, isUser = true)
        file.remotes.forEach { remotes.insert(it.copy(id = 0)) }
        messages.emit(ctx.getString(R.string.msg_done))
    }

    fun restore(uri: Uri) = viewModelScope.launch {
        try { restoreText(readText(uri)) }
        catch (e: Exception) { messages.emit(ctx.getString(R.string.msg_import_failed, e.message ?: "")) }
    }

    /** Pasted text: a Flipper .ir file, profile JSON, or a remote shared from another phone. */
    fun importPasted(text: String, category: DeviceCategory) = viewModelScope.launch {
        try {
            val t = text.trim()
            when {
                t.startsWith("Filetype", ignoreCase = true) -> {
                    val stamp = System.currentTimeMillis()
                    val cats = if (category == DeviceCategory.TV || category == DeviceCategory.SMART_TV) listOf("TV", "SMART_TV") else listOf(category.name)
                    val list = FlipperIrParser.parse(t, listOf("Pasted", "Pasted", "pasted.ir"))
                        .map { it.copy(id = it.id + "_" + stamp, categories = cats, brand = "Pasted", source = "pasted") }
                    val r = profiles.insertProfiles(list, isUser = true)
                    messages.emit(ctx.getString(R.string.msg_imported, r.imported))
                }
                t.startsWith("{") && "\"remotes\"" in t -> restoreText(t)
                else -> {
                    val r = profiles.importText(t)
                    if (r.error != null) messages.emit(ctx.getString(R.string.msg_import_failed, r.error))
                    else messages.emit(ctx.getString(R.string.msg_imported, r.imported))
                }
            }
        } catch (e: Exception) { messages.emit(ctx.getString(R.string.msg_import_failed, e.message ?: "")) }
    }
}

@HiltViewModel
class HistoryViewModel @Inject constructor(private val dao: HistoryDao) : ViewModel() {
    val items = dao.observe().stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())
    fun clear() = viewModelScope.launch { dao.clear() }
}
