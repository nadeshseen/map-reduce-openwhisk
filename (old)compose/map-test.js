const composer = require('openwhisk-composer')
   
module.exports = 
    composer.sequence(
        'splitdata-function', 
        composer.parallel(
            composer.sequence(composer.literal({value: "1"}), 'mapper-function'), 
            composer.sequence(composer.literal({value: "2"}), 'mapper-function'),
            composer.sequence(composer.literal({value: "3"}), 'mapper-function'),
        ),
        composer.parallel(
            composer.sequence(composer.literal({value: "1"}), 'reducer-function'), 
            composer.sequence(composer.literal({value: "2"}), 'reducer-function'),
            composer.sequence(composer.literal({value: "3"}), 'reducer-function'),
        ),

        composer.sequence(composer.literal({value1: "1", value2: "2", value3: "3"}), 'aggregator-function')
    )