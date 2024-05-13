FROM exasol/script-language-container:template-Exasol-all-python-3.10_release_QPAFQSHIVTUIPZW5OZKXKUK2C4PGNYPC3E2WFD72HONLVRPLUXIA
COPY release/dist /project
RUN pip install /project/*.whl


RUN mkdir -p /build_info/actual_installed_packages/release && \
    /scripts/list_installed_scripts/list_installed_apt.sh > /build_info/actual_installed_packages/release/apt_get_packages && \
    /scripts/list_installed_scripts/list_installed_pip.sh python3.10 > /build_info/actual_installed_packages/release/python3_pip_packages
