/**
 * Shared logic for table filtering and sorting.
 * Supports operators: >, <, >=, <=, =, !=
 * Supports quantity prefix: q: or q
 */

const FilterUtils = {
    parseOperator: (filterVal, itemVal) => {
        if (!filterVal) return true;
        filterVal = filterVal.toString().trim();
        itemVal = Number(itemVal) || 0;

        let operator = '=';
        let value = filterVal;

        if (filterVal.startsWith('>=')) { operator = '>='; value = filterVal.substring(2); }
        else if (filterVal.startsWith('<=')) { operator = '<='; value = filterVal.substring(2); }
        else if (filterVal.startsWith('>')) { operator = '>'; value = filterVal.substring(1); }
        else if (filterVal.startsWith('<')) { operator = '<'; value = filterVal.substring(1); }
        else if (filterVal.startsWith('=')) { operator = '='; value = filterVal.substring(1); }
        else if (filterVal.startsWith('!=')) { operator = '!='; value = filterVal.substring(2); }
        else if (filterVal.startsWith('!')) { operator = '!='; value = filterVal.substring(1); }

        const targetVal = Number(value);
        if (isNaN(targetVal)) return false; // Invalid number in filter

        switch (operator) {
            case '>': return itemVal > targetVal;
            case '<': return itemVal < targetVal;
            case '>=': return itemVal >= targetVal;
            case '<=': return itemVal <= targetVal;
            case '=': return itemVal === targetVal;
            case '!=': return itemVal !== targetVal;
            default: return false;
        }
    },

    checkPriceOrQty: (filterText, price, qty) => {
        if (!filterText) return true;
        // Check for explicit 'q' prefix for quantity
        if (filterText.toLowerCase().startsWith('q')) {
            let criteria = filterText.substring(1).trim();
            // If starts with : or space, remove it
            if (criteria.startsWith(':')) criteria = criteria.substring(1).trim();
            return FilterUtils.parseOperator(criteria, qty);
        }
        // Otherwise default to price filter
        return FilterUtils.parseOperator(filterText, price);
    }
};
