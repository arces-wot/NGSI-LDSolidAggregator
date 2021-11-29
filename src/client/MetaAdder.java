package client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import it.unibo.arces.wot.sepa.commons.exceptions.SEPABindingsException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAPropertiesException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAProtocolException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPASecurityException;
import it.unibo.arces.wot.sepa.commons.response.ErrorResponse;
import it.unibo.arces.wot.sepa.commons.response.QueryResponse;
import it.unibo.arces.wot.sepa.commons.response.Response;
import it.unibo.arces.wot.sepa.commons.sparql.ARBindingsResults;
import it.unibo.arces.wot.sepa.commons.sparql.Bindings;
import it.unibo.arces.wot.sepa.commons.sparql.BindingsResults;
import it.unibo.arces.wot.sepa.commons.sparql.RDFTermLiteral;
import it.unibo.arces.wot.sepa.commons.sparql.RDFTermURI;
import it.unibo.arces.wot.sepa.pattern.Aggregator;
import it.unibo.arces.wot.sepa.pattern.GenericClient;
import it.unibo.arces.wot.sepa.pattern.JSAP;
import it.unibo.arces.wot.sepa.pattern.Producer;

public class MetaAdder extends Aggregator {
	private String ROOT = "http://localhost:3000/";

	public MetaAdder(JSAP appProfile, String subscribeID, String updateID)
			throws SEPAProtocolException, SEPASecurityException, SEPAPropertiesException {
		super(appProfile, subscribeID, updateID);
	}

	@Override
	public void onFirstResults(BindingsResults results) {
		this.addMeta(results);
	}

	@Override
	public void onAddedResults(BindingsResults results) {
		this.addMeta(results);
	}

	private void addMeta(BindingsResults results) {
		// TODO Auto-generated method stub
		List<Bindings> data = results.getBindings();
		for (Bindings binding : data) {

			String subject = binding.getValue("g");
			String subject_parent = subject;
			String graph = "meta:" + subject;
			// TODO if already exists we don't have to do this
			if (!metaAlreadyExists(graph)) {
				try {
					Instant datetime = java.time.Clock.systemUTC().instant();
					this.setUpdateBindingValue("subject", new RDFTermURI(subject));
					this.setUpdateBindingValue("graph", new RDFTermURI(graph));
					this.setUpdateBindingValue("datetime", new RDFTermLiteral(datetime.toString()));

				} catch (SEPABindingsException e) {
					e.printStackTrace();
					continue;
				}

				try {
					update();
				} catch (SEPASecurityException | SEPAProtocolException | SEPAPropertiesException
						| SEPABindingsException e) {
					e.printStackTrace();
					continue;
				}
			}
			try {
				if (!this.isRootContainer(subject, ROOT))
					subject_parent = this.getParentContainer(subject);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			while (!this.isRootContainer(subject_parent, ROOT)) {
				// add the meta for the container
				// TODO if already exists we don't have to do this
				String metagraphcontainer = "meta:" + subject_parent;
				Producer addMetaContainer;
				String updateID = "META_ADDER_CONTAINER";
				if (!metaAlreadyExists(metagraphcontainer)) {
				try {
					Instant datetime = java.time.Clock.systemUTC().instant();
					addMetaContainer = new Producer(appProfile, updateID);
					addMetaContainer.setUpdateBindingValue("metagraphcontainer", new RDFTermURI(metagraphcontainer));
					addMetaContainer.setUpdateBindingValue("graphcontainer", new RDFTermURI(subject_parent));
					addMetaContainer.setUpdateBindingValue("datetime", new RDFTermLiteral(datetime.toString()));
					addMetaContainer.update();
					addMetaContainer.close();
				} catch (SEPAProtocolException | SEPASecurityException | SEPAPropertiesException | SEPABindingsException
						| IOException e) {
					System.err.println("Something went wrong during execution of " + updateID);
				}
				}
				// add contains property
				// TODO if already exists we don't have to do this
				Producer addContains;
				updateID = "ADD_CONTAINS";
				try {
					addContains = new Producer(appProfile, updateID);
					addContains.setUpdateBindingValue("graphcontainer", new RDFTermURI(subject_parent));
					addContains.setUpdateBindingValue("subgraph", new RDFTermURI(subject));
					addContains.update();
					addContains.close();
				} catch (SEPAProtocolException | SEPASecurityException | SEPAPropertiesException | SEPABindingsException
						| IOException e) {
					System.err.println("Something went wrong during execution of " + updateID);
				}

				try {
					subject = subject_parent;
					subject_parent = this.getParentContainer(subject);
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}

	private boolean metaAlreadyExists(String graph) {
		// TODO Auto-generated method stub
		String queryID = "ASK_GRAPH";
		boolean result= false;
		try {
			GenericClient client = new GenericClient(this.appProfile, null);
			Bindings binding= new Bindings();
			binding.addBinding("g",  new RDFTermURI(graph));
			QueryResponse res = (QueryResponse) client.query(queryID, binding);
			result = res.toString().contains("true");
			
		} catch (SEPAProtocolException | SEPASecurityException | SEPAPropertiesException | SEPABindingsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	private boolean isRootContainer(String resIdentifier, String rootId) {
		return this.ensureTrailingSlash(resIdentifier).equals(rootId);
	}

	private String ensureTrailingSlash(String str) {
		// First, remove every trailing slash:
		while (str.charAt(str.length() - 1) == '/') {
			str = str.substring(0, str.length() - 1);
		}
		// Then, add a single slash:
		str += "/";

		return str;
	}

	private URI getParentContainerFromURI(URI uri) {
		return uri.getPath().endsWith("/") ? uri.resolve("..") : uri.resolve(".");
	}

	private String getParentContainer(String resourceId) throws URISyntaxException {
		// Trailing slash is necessary for URI library
		String uriWithTrailingSlash = this.ensureTrailingSlash(resourceId);
		URI uri = new URI(uriWithTrailingSlash);
		URI parentUri = this.getParentContainerFromURI(uri);
		return parentUri.toString();
	}

	public static void main(String[] args) throws SEPAProtocolException, SEPASecurityException, SEPAPropertiesException,
			SEPABindingsException, IOException

	{

		JSAP appProfile = new JSAP("resources/SolidScorpio.jsap");

		MetaAdder app = new MetaAdder(appProfile, "NGSI", "META_ADDER");
		app.subscribe(5000L, 3L);

		synchronized (app) {
			try {
				app.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		app.close();
	}

	@Override
	public void onResults(ARBindingsResults results) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onRemovedResults(BindingsResults results) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onError(ErrorResponse errorResponse) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onSubscribe(String spuid, String alias) {
		// TODO Auto-generated method stub
		System.out.println("SUBSCRIBED SUCCESSFULLY");

	}

	@Override
	public void onUnsubscribe(String spuid) {
		// TODO Auto-generated method stub
		System.out.println("UNSUBSCRIBED SUCCESSFULLY");
	}

}
