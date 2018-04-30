module.exports = function(wallaby) {
  return {
    files: [
      {
        pattern: "node_modules/babel-polyfill/dist/polyfill.js",
        instrument: false
      },
      "**/*.js",
      "!__tests__/*.spec.js",
      "!node_modules/**/*.*"
    ],
    tests: ["__tests__/*.spec.js"],
    env: {
      type: "node"
    },
    testFramework: "jest",
    compilers: {
      '**/*.js': wallaby.compilers.babel()
    },
    workers: {
      recycle: true
    }
  }
}
