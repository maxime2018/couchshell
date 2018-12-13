package org.taktik.couch.communication

import groovy.json.JsonSlurper
import groovyx.net.http.ContentType
import groovyx.net.http.HTTPBuilder
import groovyx.net.http.Method
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.taktik.couch.state.ShellState

/** Created by aduchate on 09/08/13, 15:30 */
@Component
class CouchDBCommunication {
	@Autowired
	ShellState shellState

	def slurper = new JsonSlurper()

	private URLConnection getConnection(URL url) {
		URLConnection uc = url.openConnection()
		if (shellState.username && shellState.password) {
			String basicAuth = "Basic " + (shellState.username + ":" + shellState.password).bytes.encodeBase64().toString()
			uc.setRequestProperty("Authorization", basicAuth)
		}
		return uc
	}

	List<String> listAllViews() {
		List<String> viewNames = []
		try {
			getConnection(new URL("${shellState.serverAddress}/${shellState.selectedDatabase}/_all_docs?startkey=\"_design/\"&endkey=\"_design0\"&include_docs=true")).inputStream.withReader {
				def result = slurper.parse(it)

				result.rows.each { d ->
					def doc = d.doc._id.replaceAll('_design/', '')
					((Map) d.doc.views).keySet().each { k -> viewNames << "${doc}/${k}" }
				}
			}
		} catch (Exception e) {

		}

		return viewNames
	}

	List<String> listAllDbs() {
		List<String> dbNames = []
		new URL("${shellState.serverAddress}/_all_dbs").withReader {
			def result = slurper.parse(it)
			dbNames += result;
		}

		return dbNames
	}

	List<List<String>> getIds(String view) {
		def comps = view.split('/')
		def result = null
		getConnection(new URL("${shellState.serverAddress}/${shellState.selectedDatabase}/_design/${comps[0]}/_view/${comps[1]}?include_docs=true")).inputStream.withReader {
			def rows = slurper.parse(it).rows
			result = rows.collect {[it.doc?.id?:it.doc?._id, it.doc?._rev]}
		}
		return result
	}

	List<List<String>> getIds(String view, String start, String end, long limit = 1000, String startDocId = null) {
		def comps = view.split('/')
		def result = null
		getConnection(new URL("${shellState.serverAddress}/${shellState.selectedDatabase}/_design/${comps[0]}/_view/${comps[1]}?reduce=false&include_docs=true&startkey="+URLEncoder.encode(start,'utf8')+"&endkey="+URLEncoder.encode(end,'utf8')+"&limit="+limit+(startDocId?"&startkey_docid="+startDocId:""))).inputStream.withReader {
			def rows = slurper.parse(it).rows
			result = rows.collect {[it.doc?.id?:it.doc?._id, it.doc?._rev]}
		}
		return result
	}

	List<String> getDocs(String view, String start = null, String end = null, long limit = 1000, String startDocId = null) {
		def comps = view.split('/')
		def rows = null

		getConnection(new URL("${shellState.serverAddress}/${shellState.selectedDatabase}/_design/${comps[0]}/_view/${comps[1]}?reduce=false&include_docs=true"+(start==null?"":"&startkey="+URLEncoder.encode(start,'utf8'))+(end==null?"":"&endkey="+URLEncoder.encode(end,'utf8'))+"&limit="+limit+(startDocId?"&startkey_docid="+startDocId:""))).inputStream.withReader {
			rows = slurper.parse(it).rows
		}
		return rows
	}

	def get(String id, String rev = null, revInfos = false) {
		def res = null
		getConnection(new URL("${shellState.serverAddress}/${shellState.selectedDatabase}/${id}${rev==null?'':'?rev='+rev}${revInfos?'?revs=true&revs_info=true':''}")).inputStream.withReader {
			res = slurper.parse(it)
		}
		return res
	}


	List<String> getDocsWithIds(String[] ids) {
        def http = new HTTPBuilder( "${shellState.serverAddress}/${shellState.selectedDatabase}/_all_docs?include_docs=true")
        def rows = null
		if (shellState.username && shellState.password) { http.setHeaders([Authorization: "Basic " + (shellState.username + ":" + shellState.password).bytes.encodeBase64().toString()]) }

        http.request( Method.POST, ContentType.JSON ) { req ->
            body = ["keys":Arrays.asList(ids)]
            response.success = { resp, json ->
                rows = json.rows
            }
			response.failure = { resp, json ->
				println resp.statusLine
			}

		}
        return rows
    }

	String postBulk(def json) {
		def returnString
		def http = new HTTPBuilder( "${shellState.serverAddress}/${shellState.selectedDatabase}/_bulk_docs" )
		if (shellState.username && shellState.password) { http.setHeaders([Authorization: "Basic " + (shellState.username + ":" + shellState.password).bytes.encodeBase64().toString()]) }

		http.request( Method.POST, ContentType.JSON ) { req ->
			body = json
			response.success = { resp, j ->
				returnString = j as String
			}
		}
		return returnString
	}

	String post(def json) {
		def returnString
		def http = new HTTPBuilder( "${shellState.serverAddress}/${shellState.selectedDatabase}" )
		if (shellState.username && shellState.password) { http.setHeaders([Authorization: "Basic " + (shellState.username + ":" + shellState.password).bytes.encodeBase64().toString()]) }

		http.request( Method.POST, ContentType.JSON ) { req ->
			body = json
			response.success = { resp, j ->
				returnString = j as String
			}
		}
		return returnString
	}



}
