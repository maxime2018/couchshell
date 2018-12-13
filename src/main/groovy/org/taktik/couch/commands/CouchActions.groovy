package org.taktik.couch.commands

import groovy.json.JsonOutput
import net.sf.json.JSONSerializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.shell.core.annotation.CliAvailabilityIndicator
import org.springframework.shell.core.annotation.CliCommand
import org.springframework.shell.core.annotation.CliOption
import org.springframework.stereotype.Component
import org.taktik.couch.communication.CouchDBCommunication

import javax.script.Compilable
import javax.script.CompiledScript
import javax.script.ScriptEngine
import javax.script.ScriptEngineManager

/** Created by aduchate on 09/08/13, 15:00 */
@Component
class CouchActions extends CouchBase {
    @Autowired
    CouchDBCommunication couchDBCommunication

    @CliAvailabilityIndicator(["delete", "list", "update"])
    public boolean areActionsAvailable() {
        return shellState.serverAddress && shellState.selectedDatabase;
    }

    @CliCommand(value = "delete from-view", help = "Delete documents from view")
    public String deleteView(
            @CliOption(key = ["view"], mandatory = true, help = "The view", optionContext = "disable-string-converter couch-view") final String view,
            @CliOption(key = ["from"], mandatory = false, help = "Start key") final String startKey,
            @CliOption(key = ["to"], mandatory = false, help = "End key") final String endKey) {
        String v = view.trim()

        startKey && endKey ?
                couchDBCommunication.postBulk([docs: couchDBCommunication.getIds(v, startKey, endKey).collect {
                    [_id: it[0], _rev: it[1], _deleted: true]
                }]) :
                couchDBCommunication.postBulk([docs: couchDBCommunication.getIds(v).collect {
                    [_id: it[0], _rev: it[1], _deleted: true]
                }])
    }

    @CliCommand(value = "delete ids", help = "Delete documents with ids")
    public String deleteIds(
            @CliOption(key = ["", "ids"], mandatory = true, help = "The ids as [id1:rev1,id2:rev2]", optionContext = "couch-ids") final String ids) {
        String i = ids.trim()
        if (i.startsWith('[') && i.endsWith(']')) {
            couchDBCommunication.postBulk([docs: i[1..-2].split(/,/).collect {
                [_id: it.split(/:/)[0], _rev: it.split(/:/)[1], _deleted: true]
            }])
        }
    }

    @CliCommand(value = "list ids", help = "List ids from view")
    public String listIdsFromViews(
            @CliOption(key = ["view"], mandatory = true, help = "The view", optionContext = "disable-string-converter couch-view") final String view,
            @CliOption(key = ["from"], mandatory = false, help = "Start key") final String startKey,
            @CliOption(key = ["to"], mandatory = false, help = "End key") final String endKey,
            @CliOption(key = ["limit"], mandatory = false, help = "Limit") final String limit,
            @CliOption(key = ["fromdoc"], mandatory = false, help = "First doc id (use in conjunction with from)") final String startDocId) {
        String v = view.trim()
        return couchDBCommunication.getIds(v, startKey ?: "null", endKey ?: "{}", limit ? Long.valueOf(limit) : 10000000, startDocId).collect {
            "${it[0]}:${it[1]}"
        }.join(",")
    }

    @CliCommand(value = "update docs", help = "update docs in view")
    public String updateFromView(
            @CliOption(key = ["view"], mandatory = true, help = "The view", optionContext = "disable-string-converter couch-view") final String view,
            @CliOption(key = ["from"], mandatory = false, help = "Start key") final String startKey,
            @CliOption(key = ["to"], mandatory = false, help = "End key") final String endKey,
            @CliOption(key = ["limit"], mandatory = false, help = "Limit") final String limit,
            @CliOption(key = ["fromdoc"], mandatory = false, help = "First doc id (use in conjunction with from)") final String startDocId,
            @CliOption(key = ["action"], mandatory = true, help = "The groovy transform to apply on each document doc") final String action) {

        def script = new GroovyShell().parse(action);

        String v = view.trim()

        return couchDBCommunication.postBulk([docs: (startKey || endKey || limit ? couchDBCommunication.getDocs(v, startKey ?: "null", endKey ?: "{}", limit ? Long.valueOf(limit) : 10000000, startDocId) : couchDBCommunication.getDocs(v)).collect {
            script.binding = new Binding(doc: it.doc); script.run()
        }.findAll { it != null }])
    }

    @CliCommand(value = "display docs", help = "display docs from view")
    public String displayFromView(
            @CliOption(key = ["view"], mandatory = true, help = "The view", optionContext = "disable-string-converter couch-view") final String view,
            @CliOption(key = ["from"], mandatory = false, help = "Start key") final String startKey,
            @CliOption(key = ["to"], mandatory = false, help = "End key") final String endKey,
            @CliOption(key = ["limit"], mandatory = false, help = "Limit") final String limit,
            @CliOption(key = ["fromdoc"], mandatory = false, help = "First doc id (use in conjunction with from)") final String startDocId,
            @CliOption(key = ["collector"], mandatory = true, help = "The groovy transform to apply on each document doc") final String action) {

        def script = new GroovyShell().parse(action);

        String v = view.trim()

        return JSONSerializer.toJSON((startKey || endKey || limit ? couchDBCommunication.getDocs(v, startKey ?: "null", endKey ?: "{}", limit ? Long.valueOf(limit) : 10000000, startDocId) : couchDBCommunication.getDocs(v)).collect {
            script.binding = new Binding(doc: it.doc); script.run()
        }.findAll { it != null })
    }

    @CliCommand(value = "update docids", help = "update docs with ids")
    public String updateFromView(
            @CliOption(key = ["ids"], mandatory = true, help = "The ids, comma separated", optionContext = "disable-string-converter couch-view") final String ids,
            @CliOption(key = ["action"], mandatory = true, help = "The groovy transform to apply on each document doc") final String action) {

        def script = new GroovyShell().parse(action);

        return couchDBCommunication.postBulk([docs: couchDBCommunication.getDocsWithIds(ids.trim().split(",")).collect {
            script.binding = new Binding(doc: it.doc);
            script.run()
        }.findAll { it != null }]);
    }

    @CliCommand(value = "revert docids", help = "revert docs with ids to previous revision")
    String revert(
            @CliOption(key = ["ids"], mandatory = true, help = "The ids, comma separated", optionContext = "disable-string-converter couch-view") final String ids,
            @CliOption(key = ["back"], mandatory = false, help = "The number of revisions to go back", optionContext = "disable-string-converter couch-view") final String back
    ) {
        Integer backRevs = back ? back.toInteger() : 1
        return couchDBCommunication.postBulk([docs: ids.trim().split(",").collect { id ->
            try {
                def initialDoc = couchDBCommunication.get(id, null, true)
                def doc = initialDoc._revs_info?.size() > backRevs && initialDoc._revs_info[backRevs].status == 'available' ? couchDBCommunication.get(id, initialDoc._revs_info[backRevs].rev) : null
                if (doc) {
                    doc._rev = initialDoc._rev
                    return doc
                }
            } catch (Exception ignored) {
                //Doc does not exist
                couchDBCommunication.post([_id: id])
                def initialDoc = couchDBCommunication.get(id, null, true)
                def doc = initialDoc._revs_info?.size() > backRevs + 2 && initialDoc._revs_info[backRevs + 2].status == 'available' ? couchDBCommunication.get(id, initialDoc._revs_info[backRevs + 2].rev) : null
                if (doc) {
                    doc._rev = initialDoc._rev
                    return doc
                }
            }
            null
        }.findAll { it != null }])
    }

    @CliCommand(value = "previous docids", help = "show previous revision")
    String showPrevious(
            @CliOption(key = ["ids"], mandatory = true, help = "The ids, comma separated", optionContext = "disable-string-converter couch-view") final String ids,
            @CliOption(key = ["back"], mandatory = false, help = "The number of revisions to go back", optionContext = "disable-string-converter couch-view") final String back
    ) {
        Integer backRevs = back ? back.toInteger() : 1
        return JsonOutput.prettyPrint(JsonOutput.toJson(ids.trim().split(",").collect { id ->
            try {
                def initialDoc = couchDBCommunication.get(id, null, true)
                def doc = initialDoc._revs_info?.size() > backRevs && initialDoc._revs_info[backRevs].status == 'available' ? couchDBCommunication.get(id, initialDoc._revs_info[backRevs].rev) : null
                if (doc) {
                    doc._rev = initialDoc._rev
                    return doc
                }
            } catch (Exception ignored) {
                //Doc does not exist
                couchDBCommunication.post([_id: id])
                def initialDoc = couchDBCommunication.get(id, null, true)
                def doc = initialDoc._revs_info?.size() > backRevs + 2 && initialDoc._revs_info[backRevs + 2].status == 'available' ? couchDBCommunication.get(id, initialDoc._revs_info[backRevs + 2].rev) : null
                if (doc) {
                    doc._rev = initialDoc._rev
                    return doc
                }
            }
            null
        }.findAll { it != null }))
    }


}
