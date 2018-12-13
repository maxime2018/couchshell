package org.taktik.couch.commands

import org.springframework.shell.core.annotation.CliAvailabilityIndicator
import org.springframework.shell.core.annotation.CliCommand
import org.springframework.shell.core.annotation.CliOption
import org.springframework.stereotype.Component
/** Created by aduchate on 09/08/13, 14:18 */
@Component
class CouchConnect extends CouchBase {
	@CliAvailabilityIndicator(["server connect"])
	public boolean isConnectAvailable() {
		return !shellState.serverAddress;
	}

	@CliAvailabilityIndicator(["server disconnect","dbselect"])
	public boolean isOthersAvailable() {
		return shellState.serverAddress;
	}

	@CliCommand(value = "server connect", help = "Connect to a server hostname[:port]")
	public String connect( @CliOption(key = [""], mandatory = true, help = "The server") final String server) {
		shellState.serverAddress = server + (server.matches('.+:[0-9]+') ? '' : ':5984')
		return "Connect to ${shellState.serverAddress} OK"
	}

	@CliCommand(value = "server disconnect", help = "Disconnect from the server")
	public String disconnect() {
		String addr = shellState.serverAddress
		shellState.serverAddress = null
		return "Disconnect from ${addr} OK"
	}

	@CliCommand(value = "dbselect", help = "Select db")
	public String dbSelect( @CliOption(key = ["database"], mandatory = true, help = "The database", optionContext = "disable-string-converter couch-db") final String db,
							@CliOption(key = ["username"], mandatory = false, help = "User name") final String username,
							@CliOption(key = ["password"], mandatory = false, help = "Password") final String password) {
		shellState.selectedDatabase = db
		shellState.username = username
		shellState.password = password
		return "${db} selected"
	}

}
