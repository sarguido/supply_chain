:BEGIN
CREATE CONSTRAINT ON (u:User) ASSERT u.id IS UNIQUE;
CREATE CONSTRAINT ON (p:Product) ASSERT p.id IS UNIQUE;
:COMMIT

:BEGIN
CALL apoc.periodic.iterate("
    load csv with headers from 'file:///JD_user_data.csv' as row
    return row
", "
    merge (u:User {
        id: row.user_ID,
        customer_since: date(row.first_order_month),
        past_purchase_ranking: toInteger(row.user_level),
        gender: row.gender,
        age: row.age,
        membership: toInteger(row.plus),
        marital_status: row.marital_status,
        education: toInteger(row.education),
        purchase_power: toInteger(row.purchase_power),
        city_size: toInteger(row.city_level)
})", {batchSize:10000});
:COMMIT

:BEGIN
CALL apoc.periodic.iterate("
    load csv with headers from 'file:///JD_sku_edit.csv' as row
    return row
", "
    merge (p:Product {
        id: row.sku_ID,
        seller_type: toInteger(row.type),
        attribute1: toInteger(row.attribute1),
        attribute2: toInteger(row.attribute2),
        brand: row.brand_ID
})", {batchSize:10000});
:COMMIT

:BEGIN
call apoc.periodic.iterate("
    load csv with headers from 'file:///JD_order_data.csv' as row
    return row
", "
    match (u:User) where u.id = row.user_ID
    match (p:Product) where p.id = row.sku_ID
    create (u)-[:ORDERED {
        quantity: toInteger(row.quantity),
        order_date: date(row.order_date),
        exp_delivery_days: toInteger(row.promise),
        original_unit_price: toFloat(row.original_unit_price),
        final_unit_price: toFloat(row.final_unit_price),
        direct_discount: toFloat(row.direct_discount_per_unit),
        quantity_discount: toFloat(row.quantity_discount_per_unit),
        bundle_discount: toFloat(row.bundle_discount_per_unit),
        coupon_discount: toFloat(row.direct_discount_per_unit),
        gift: toInteger(row.gift_item)
    }]->(p)
", {batchSize: 10000});
:COMMIT
