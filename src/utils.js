import R from 'ramda'
import snakeCase from 'lodash.snakecase'
import queryBuilder from './query-builder'
import filterBuilder from './filter-builder'

/**
 * Compound sort function into the list of sorts
 *
 * @private
 *
 * @param  {Array} current Array of Elasticsearch sorts.
 * @param  {String} field Field to sort.
 * @param  {String|Object} value A valid direction ('asc', 'desc') or object with sort options
 * @returns {Array} Array of Elasticsearch sorts.
 */
export function sortMerge(current, field, value) {
  let payload

  if (R.is(Object, value)) {
    payload = { [field]: R.clone(value) }
  } else {
    payload = { [field]: { order: value } }
  }

  return R.append(payload, current)
}

/**
 * Generic builder for query, filter, or aggregation clauses.
 *
 * @private
 *
 * @param  {string|Object} field Field name or complete clause.
 * @param  {string|Object} value Field value or inner clause.
 * @param  {Object}        opts  Additional key-value pairs.
 *
 * @return {Object} Clause
 */
export function buildClause (field, value, opts) {
  const hasField = field != null
  const hasValue = value != null
  let mainClause = {}

  if (hasValue) {
    mainClause = {[field]: value}
  } else if (R.is(Object, field)) {
    mainClause = field
  } else if (hasField) {
    mainClause = {field}
  }

  return Object.assign({}, mainClause, opts)
}

export function toBool (filters) {
  const unwrapped = {
    must: unwrap(filters.and),
    should: unwrap(filters.or),
    must_not: unwrap(filters.not),
    minimum_should_match: filters.minimum_should_match
  }

  if (
    filters.and.length === 1 &&
    !unwrapped.should &&
    !unwrapped.must_not
  ) {
    return unwrapped.must
  }

  const cleaned = {}

  if (unwrapped.must) {
    cleaned.must = unwrapped.must
  }
  if (unwrapped.should) {
    cleaned.should = filters.or
  }
  if (unwrapped.must_not) {
    cleaned.must_not = filters.not
  }
  if (
    unwrapped.minimum_should_match &&
    filters.or.length > 1
  ) {
    cleaned.minimum_should_match = unwrapped.minimum_should_match
  }

  return {
    bool: cleaned
  }
}

function unwrap (arr) {
  return arr.length > 1 ? arr : R.last(arr)
}

const nestedTypes = ['nested', 'has_parent', 'has_child']

export function pushQuery (existing, boolKey, type, ...args) {
  const nested = {}
  if (R.is(Function, R.last(args))) {
    const isNestedType = R.contains(snakeCase(type), nestedTypes)
    const nestedCallback = args.pop()
    // It is illogical to add a query nested inside a filter, because its
    // scoring won't be taken into account by elasticsearch. However we do need
    // to provide the `query` methods in the context of joined queries for
    // backwards compatability.
    const nestedResult = nestedCallback(
      Object.assign(
        {},
        filterBuilder({ isInFilterContext: this.isInFilterContext }),
        (this.isInFilterContext && !isNestedType)
          ? {}
          : queryBuilder({ isInFilterContext: this.isInFilterContext })
      )
    )
    if (isNestedType) {
      nested.query = build(
        {},
        nestedResult.getQuery(),
        nestedResult.getFilter()
      ).query
    } else {
      if (!this.isInFilterContext && nestedResult.hasQuery()) {
        nested.must = nestedResult.getQuery()
      }
      if (nestedResult.hasFilter()) {
        nested.filter = nestedResult.getFilter()
      }
    }
  }

  if (
    R.contains(type, ['bool', 'constant_score']) &&
    this.isInFilterContext &&
    R.path(['filter', 'bool'], nested)
  ) {
    // nesting filters: We've introduced an unnecessary `filter.bool`
    existing[boolKey].push(
      {[type]: Object.assign(buildClause(...args), nested.filter.bool)}
    )
  } else {
    // Usual case
    existing[boolKey].push(
      {[type]: Object.assign(buildClause(...args), nested)}
    )
  }
}

export function buildV1(body, queries, filters, aggregations) {
  let clonedBody = R.clone(body)

  if (!R.isEmpty(filters)) {
    clonedBody = R.assocPath(['query', 'filtered', 'filter'], filters, clonedBody)

    if (!R.isEmpty(queries)) {
      clonedBody = R.assocPath(['query', 'filtered', 'query'], queries, clonedBody)
    }

  } else if (!R.isEmpty(queries)) {
    clonedBody = R.assoc('query', queries, clonedBody)
  }

  if (!R.isEmpty(aggregations)) {
    clonedBody = R.assoc('aggregations', aggregations, clonedBody)
  }
  return clonedBody
}

export function build(body, queries, filters, aggregations) {
  let clonedBody = R.clone(body)

  if (!R.isEmpty(filters)) {
    let filterBody = {}
    let queryBody = {}
    filterBody = R.assocPath(['query', 'bool', 'filter'], filters, filterBody)
    if (!R.isEmpty(queries.bool)) {
      queryBody = R.assocPath(['query', 'bool'], queries.bool, queryBody)
    } else if (!R.isEmpty(queries)) {
      R.assocPath(['query', 'bool', 'must'], queries.bool, queryBody)
    }
    clonedBody = R.reduce(R.mergeDeepRight, clonedBody, [filterBody, queryBody])
  } else if (!R.isEmpty(queries)) {
    clonedBody = R.assoc('query', queries, clonedBody)
  }

  if (!R.isEmpty(aggregations)) {
    clonedBody = R.assoc('aggs', aggregations, clonedBody)
  }

  return clonedBody
}
