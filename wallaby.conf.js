module.exports = function(wallaby) {
  return {
    files: [
      {
        pattern: "node_modules/babel-polyfill/dist/polyfill.js",
        instrument: false
      },
      "src/**/*.js",
      "!**/*.spec.js",
      "!node_modules/**/*.*"
    ],
    tests: ["**/*.spec.js", "!node_modules/**/*.*"],
    env: {
      type: "node",
      runner: "node",
      params: {
        env: "KUBE_SERVICE_ACCOUNT_TOKEN=MOCK_TOKEN;BUCKET=test.bucket"
      }
    },
    testFramework: "jest",
    setup: function() {
      var jestConfig = require("./package.json").jest;

      wallaby.testFramework.configure(jestConfig);
    },
    preprocessors: {
      "**/*.js": file =>
        require("@babel/core").transform(file.content, {
          sourceMap: true,
          filename: file.path,
          presets: [require("babel-preset-jest")]
        })
    },
    compilers: {
      "src/**/*.js": wallaby.compilers.babel()
    },
    workers: {
      recycle: true
    },
    debug: true
  };
};
