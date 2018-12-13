package org.taktik.couch.state

import org.springframework.stereotype.Component

/** Created by aduchate on 09/08/13, 15:02 */
@Component
class ShellState {
	String protocol
	String serverAddress
	String selectedDatabase
    String username
	String password
}
