package org.m2mp.es;

import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 *
 * @author Florent Clairambault
 */
public class EsSearchRequest {

	private QueryBuilder query;

	public void setQuery(QueryBuilder query) {
		this.query = query;
	}
	private final List<SortBuilder> sorts = new ArrayList<>();

	public void addSort(String fieldName) {
		addSort(SortBuilders.fieldSort(fieldName));
	}

	public void addSort(String fieldName, boolean order) {
		addSort(SortBuilders.fieldSort(fieldName).order(order ? SortOrder.ASC : SortOrder.DESC));
	}

	public void addSort(SortBuilder sb) {
		sorts.add(sb);
	}
	private final List<AbstractFacetBuilder> facets = new ArrayList<>();

	public void addFacetsTerms(String fieldName) {
		facets.add(FacetBuilders.termsFacet(fieldName).field(fieldName));
	}
	private int from = -1;

	public void setFrom(int from) {
		this.from = from;
	}
	private int size = -1;

	public void setSize(int size) {
		this.size = size;
	}

	public SearchRequestBuilder getSearchRequest() {
		SearchRequestBuilder search = new SearchRequestBuilder(null);
		search.setQuery(query);
		for (AbstractFacetBuilder afb : facets) {
			search.addFacet(afb);
		}
		for (SortBuilder sb : sorts) {
			search.addSort(sb);
		}
		if (size != -1) {
			search.setSize(size);
		}
		if (from != -1) {
			search.setFrom(from);
		}
		return search;
	}

	@Override
	public String toString() {
		return getSearchRequest().toString();
	}
}
