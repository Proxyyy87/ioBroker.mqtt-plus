// ioBroker prettier configuration file
import prettierConfig from '@iobroker/eslint-config/prettier.config.mjs';

export default {
    ...prettierConfig,
    // the code base uses double quotes
    singleQuote: false,
};
