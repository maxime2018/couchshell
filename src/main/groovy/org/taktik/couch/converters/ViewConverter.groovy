package org.taktik.couch.converters
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.shell.core.Completion
import org.springframework.shell.core.Converter
import org.springframework.shell.core.MethodTarget
import org.springframework.stereotype.Component
import org.taktik.couch.communication.CouchDBCommunication

import java.util.logging.Level
import java.util.logging.Logger

/** Created by aduchate on 09/08/13, 15:26 */
@Component
class ViewConverter implements Converter<String> {
	protected final Logger LOG = Logger.getLogger(getClass().getName());
	@Autowired
	CouchDBCommunication couchDBCommunication

	@Override
	boolean supports(Class<?> type, String optionContext) {
		LOG.log(Level.SEVERE,"Trying to list views for: existingData")
		return type.equals(String.class) && optionContext.equals("couch-view")
	}

	@Override
	String convertFromText(String value, Class<?> targetType, String optionContext) {
		return value
	}

	@Override
	boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType, String existingData, String optionContext, MethodTarget target) {
		LOG.log(Level.SEVERE,"Trying to list views for: existingData")

		def all = couchDBCommunication.listAllViews().findAll { it.startsWith(existingData) }
		completions.addAll(all.collect {new Completion(it)});
		return all.size()==0||(all.size()==1 && all[0].equals(existingData))
	}


}
